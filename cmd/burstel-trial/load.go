package main

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"sync"
	"time"
)

// routeSpec is one weighted workload target.
type routeSpec struct {
	path   string
	weight float64
}

type routeStat struct {
	Attempted       int64 `json:"attempted"`
	Success         int64 `json:"success"`
	DeliberateError int64 `json:"deliberate_error"`
	UnexpectedError int64 `json:"unexpected_error"`
	TransportError  int64 `json:"transport_error"`
}

type latencyStats struct {
	Count int64   `json:"count"`
	Mean  float64 `json:"mean_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
}

type emittedCounts struct {
	ServerSpans        int64 `json:"server_spans"`
	DBSpans            int64 `json:"db_spans"`
	LogRecords         int64 `json:"log_records"`
	MetricMeasurements int64 `json:"metric_measurements"`
}

// loadReport is the machine-readable outcome of one workload run.
type loadReport struct {
	Mode               string               `json:"mode"`
	ServiceName        string               `json:"service_name"`
	AppBaseURL         string               `json:"app_base_url"`
	TargetRPS          float64              `json:"target_rps"`
	Workers            int                  `json:"workers"`
	DurationTargetSecs float64              `json:"duration_target_seconds"`
	DurationActualSecs float64              `json:"duration_actual_seconds"`
	Attempted          int64                `json:"attempted"`
	Success            int64                `json:"success"`
	DeliberateErrors   int64                `json:"deliberate_errors"`
	UnexpectedErrors   int64                `json:"unexpected_errors"`
	TransportErrors    int64                `json:"transport_errors"`
	ErrorRate          float64              `json:"unexpected_error_rate"`
	Latency            latencyStats         `json:"latency"`
	ByRoute            map[string]routeStat `json:"by_route"`
	ByStatus           map[string]int64     `json:"by_status"`
	Emitted            emittedCounts        `json:"emitted"`
	Export             exportSnapshot       `json:"otlp_export"`
	BreakerTripped     bool                 `json:"breaker_tripped"`
	StartedAt          time.Time            `json:"started_at"`
	EndedAt            time.Time            `json:"ended_at"`
	Notes              []string             `json:"notes,omitempty"`
}

type loadResult struct {
	report *loadReport
	app    *trialApp
}

// runLoad drives bounded HTTP load against the in-process application using a
// fixed target rate and a worker pool. It stops on the duration deadline, on
// context cancellation, or when the sustained unexpected-error rate exceeds
// maxErrorRate (a deliberate /api/error response does not trip the breaker).
func runLoad(ctx context.Context, baseURL string, cfg trialConfig, app *trialApp) *loadReport {
	routes := buildRoutes(cfg)

	client := &http.Client{
		Timeout: 15 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        128,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	period := time.Duration(float64(time.Second) / cfg.rate)
	if period < time.Millisecond {
		period = time.Millisecond
	}
	ticker := time.NewTicker(period)
	defer ticker.Stop()

	jobs := make(chan string, cfg.workers*4)

	var (
		mu          sync.Mutex
		byRoute     = map[string]*routeStat{}
		byStatus    = map[string]int64{}
		attempted   int64
		success     int64
		delibErrors int64
		unexpErrors int64
		transErrors int64
		latencies   []float64
		breaker     bool
	)

	record := func(path string, status string, durMs float64, kind string) {
		mu.Lock()
		defer mu.Unlock()
		rs := byRoute[path]
		if rs == nil {
			rs = &routeStat{}
			byRoute[path] = rs
		}
		rs.Attempted++
		attempted++
		switch kind {
		case "success":
			rs.Success++
			success++
		case "deliberate":
			rs.DeliberateError++
			delibErrors++
		case "unexpected":
			rs.UnexpectedError++
			unexpErrors++
		case "transport":
			rs.TransportError++
			transErrors++
		}
		byStatus[status]++
		latencies = append(latencies, durMs)
	}

	var wg sync.WaitGroup
	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				req, err := http.NewRequestWithContext(runCtx, http.MethodGet, baseURL+path, nil)
				if err != nil {
					record(path, "request-error", 0, "transport")
					continue
				}
				begin := time.Now()
				resp, err := client.Do(req)
				durMs := float64(time.Since(begin).Microseconds()) / 1000.0
				if err != nil {
					record(path, "transport-error", durMs, "transport")
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				switch {
				case resp.StatusCode == http.StatusOK:
					record(path, fmt.Sprint(resp.StatusCode), durMs, "success")
				case path == "/api/error" && resp.StatusCode == http.StatusInternalServerError:
					// The deliberate failure regime is expected, not an incident.
					record(path, fmt.Sprint(resp.StatusCode), durMs, "deliberate")
				default:
					record(path, fmt.Sprint(resp.StatusCode), durMs, "unexpected")
				}
			}
		}()
	}

	started := time.Now()
	deadline := started.Add(cfg.duration)
dispatch:
	for {
		select {
		case <-runCtx.Done():
			break dispatch
		case <-ticker.C:
			if time.Now().After(deadline) {
				break dispatch
			}
			path := pickRoute(routes)
			select {
			case jobs <- path:
			case <-runCtx.Done():
				break dispatch
			default:
				// Worker pool saturated: skip this tick rather than queue
				// unbounded work. The skipped tick is reflected as a lower
				// attempted count, not as data loss.
			}
			mu.Lock()
			tripped := attempted >= 200 && float64(unexpErrors+transErrors)/float64(attempted) > cfg.maxErrorRate
			if tripped {
				breaker = true
			}
			mu.Unlock()
			if tripped {
				break dispatch
			}
		}
	}
	close(jobs)
	wg.Wait()
	ended := time.Now()

	rep := &loadReport{
		Mode:               cfg.mode,
		ServiceName:        cfg.serviceName,
		AppBaseURL:         baseURL,
		TargetRPS:          cfg.rate,
		Workers:            cfg.workers,
		DurationTargetSecs: cfg.duration.Seconds(),
		DurationActualSecs: ended.Sub(started).Seconds(),
		Attempted:          attempted,
		Success:            success,
		DeliberateErrors:   delibErrors,
		UnexpectedErrors:   unexpErrors,
		TransportErrors:    transErrors,
		ByRoute:            map[string]routeStat{},
		ByStatus:           map[string]int64{},
		BreakerTripped:     breaker,
		StartedAt:          started,
		EndedAt:            ended,
	}
	if attempted > 0 {
		rep.ErrorRate = round3(float64(unexpErrors+transErrors) / float64(attempted))
	}
	for k, v := range byRoute {
		rep.ByRoute[k] = *v
	}
	for k, v := range byStatus {
		rep.ByStatus[k] = v
	}
	rep.Latency = computeLatency(latencies)
	if breaker {
		rep.Notes = append(rep.Notes, "load generator circuit breaker tripped on sustained unexpected errors")
	}
	return rep
}

func buildRoutes(cfg trialConfig) []routeSpec {
	errorW := cfg.errorRate
	summaryW := 0.25
	itemsW := 1.0 - errorW - summaryW
	if itemsW < 0.1 {
		itemsW = 0.1
	}
	total := itemsW + summaryW + errorW
	return []routeSpec{
		{path: "/api/items/42", weight: itemsW / total},
		{path: "/api/summary", weight: summaryW / total},
		{path: "/api/error", weight: errorW / total},
	}
}

func pickRoute(routes []routeSpec) string {
	if len(routes) == 0 {
		return "/api/summary"
	}
	// A deterministic-but-varying pick is enough; the weights only shape the
	// mix, they are not a statistical guarantee.
	r := (time.Now().UnixNano() / 1000) % 1000
	u := float64(r) / 1000.0
	cum := 0.0
	for _, rs := range routes {
		cum += rs.weight
		if u <= cum {
			return rs.path
		}
	}
	return routes[len(routes)-1].path
}

func computeLatency(samples []float64) latencyStats {
	if len(samples) == 0 {
		return latencyStats{}
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	// Nearest-rank percentile: rank = ceil(p * n), clamped to [1, n].
	q := func(p float64) float64 {
		rank := int(math.Ceil(p * float64(len(sorted))))
		if rank < 1 {
			rank = 1
		}
		if rank > len(sorted) {
			rank = len(sorted)
		}
		return sorted[rank-1]
	}
	return latencyStats{
		Count: int64(len(sorted)),
		Mean:  round3(sum / float64(len(sorted))),
		P50:   round3(q(0.50)),
		P95:   round3(q(0.95)),
		P99:   round3(q(0.99)),
		Max:   round3(sorted[len(sorted)-1]),
	}
}

func round3(f float64) float64 {
	return float64(int64(f*1000+0.5)) / 1000
}
