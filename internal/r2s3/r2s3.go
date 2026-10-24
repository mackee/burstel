// Package r2s3 is a tiny, dependency-free Cloudflare R2 S3 client.
//
// Spec 031 grants the compactor/materializer Container a bucket-scoped R2
// credential so shot-time materialization can read journal objects and write
// part files directly, instead of streaming every byte through a Durable
// Object. Only the three operations the materializer needs are implemented:
// GET, PUT and HEAD, each signed with AWS Signature Version 4.
package r2s3

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// Config describes one bucket-scoped R2 credential.
type Config struct {
	// AccountID is the Cloudflare account id; the S3 endpoint is derived from
	// it unless Endpoint is set.
	AccountID string
	// AccessKeyID / SecretAccessKey are the scoped R2 token pair.
	AccessKeyID     string
	SecretAccessKey string
	// Bucket is the single bucket the credential is allowed to touch.
	Bucket string
	// Endpoint overrides the derived endpoint (tests and local fakes).
	Endpoint string
	// Region is the S3 signing region. R2 uses "auto".
	Region string
	// HTTPClient is optional; the default client is used when nil.
	HTTPClient *http.Client
	// Now is an injectable clock for deterministic signing tests.
	Now func() time.Time
}

// Client is a bucket-scoped R2 S3 client. It is safe for concurrent use.
type Client struct {
	endpoint  string
	bucket    string
	accessKey string
	secretKey string
	region    string
	http      *http.Client
	now       func() time.Time
}

// New validates cfg and builds a client. A missing endpoint is derived from the
// account id. The bucket is required: an unscoped client is never constructed.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.Bucket) == "" {
		return nil, fmt.Errorf("r2s3: bucket is required")
	}
	if strings.TrimSpace(cfg.AccessKeyID) == "" || strings.TrimSpace(cfg.SecretAccessKey) == "" {
		return nil, fmt.Errorf("r2s3: access key id and secret access key are required")
	}
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.Endpoint), "/")
	if endpoint == "" {
		account := strings.TrimSpace(cfg.AccountID)
		if account == "" {
			return nil, fmt.Errorf("r2s3: account id or endpoint is required")
		}
		endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", account)
	}
	region := strings.TrimSpace(cfg.Region)
	if region == "" {
		region = "auto"
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	now := cfg.Now
	if now == nil {
		now = time.Now
	}
	return &Client{
		endpoint:  endpoint,
		bucket:    cfg.Bucket,
		accessKey: cfg.AccessKeyID,
		secretKey: cfg.SecretAccessKey,
		region:    region,
		http:      client,
		now:       now,
	}, nil
}

// Bucket returns the scoped bucket name.
func (c *Client) Bucket() string { return c.bucket }

// Get downloads one object.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	resp, err := c.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("r2s3: read %s: %w", key, err)
	}
	return data, nil
}

// GetTo streams one object into w and returns the number of bytes written.
//
// Spec 035: the analysis container downloads materialization parts directly
// from R2 and verifies the SHA-256 while writing, so it needs a streaming GET
// that never holds the whole part in memory.
func (c *Client) GetTo(ctx context.Context, key string, w io.Writer) (int64, error) {
	resp, err := c.do(ctx, http.MethodGet, key, nil, nil)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close() //nolint:errcheck
	n, err := io.Copy(w, resp.Body)
	if err != nil {
		return n, fmt.Errorf("r2s3: read %s: %w", key, err)
	}
	return n, nil
}

// Put uploads one object, replacing any existing value at key.
func (c *Client) Put(ctx context.Context, key string, data []byte, contentType string) error {
	headers := map[string]string{}
	if contentType != "" {
		headers["content-type"] = contentType
	}
	resp, err := c.do(ctx, http.MethodPut, key, data, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close() //nolint:errcheck
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Head returns the object size and ETag without downloading the body.
func (c *Client) Head(ctx context.Context, key string) (int64, string, error) {
	resp, err := c.do(ctx, http.MethodHead, key, nil, nil)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	return resp.ContentLength, strings.Trim(resp.Header.Get("ETag"), `"`), nil
}

func (c *Client) do(ctx context.Context, method, key string, body []byte, extra map[string]string) (*http.Response, error) {
	if key == "" {
		return nil, fmt.Errorf("r2s3: object key is required")
	}
	u, err := url.Parse(c.endpoint + canonicalPath(c.bucket, key))
	if err != nil {
		return nil, fmt.Errorf("r2s3: build url: %w", err)
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), reader)
	if err != nil {
		return nil, fmt.Errorf("r2s3: build request: %w", err)
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	payloadHash := sha256Hex(body)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	sign(req, c.accessKey, c.secretKey, c.region, payloadHash, c.now().UTC())
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("r2s3: %s %s: %w", method, key, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("r2s3: %s %s returned %d: %s", method, key, resp.StatusCode, strings.TrimSpace(string(detail)))
	}
	return resp, nil
}

func sign(req *http.Request, accessKey, secretKey, region, payloadHash string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	day := now.Format("20060102")
	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("host", req.URL.Host)

	headers := map[string]string{
		"host":                 req.URL.Host,
		"x-amz-content-sha256": payloadHash,
		"x-amz-date":           amzDate,
	}
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	var canonicalHeaders strings.Builder
	for _, name := range names {
		canonicalHeaders.WriteString(name)
		canonicalHeaders.WriteByte(':')
		canonicalHeaders.WriteString(strings.TrimSpace(headers[name]))
		canonicalHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		canonicalHeaders.String(),
		signedHeaders,
		payloadHash,
	}, "\n")
	scope := strings.Join([]string{day, region, "s3", "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+secretKey), day)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, "s3")
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))
	req.Header.Set("authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		accessKey, scope, signedHeaders, signature,
	))
}

func canonicalPath(bucket, key string) string {
	parts := strings.Split(key, "/")
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return "/" + url.PathEscape(bucket) + "/" + strings.Join(parts, "/")
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(data))
	return mac.Sum(nil)
}
