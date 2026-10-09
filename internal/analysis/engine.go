package analysis

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/duckdb/duckdb-go/v2"
)

// maxSafeJSONInt is the largest integer an IEEE-754 double represents exactly.
const maxSafeJSONInt = int64(1)<<53 - 1

// Engine is a read-only DuckDB connection to one restored frozen shot.
type Engine struct {
	db         *sql.DB
	path       string
	schema     string
	configNote []string
	mu         sync.Mutex
	closed     bool
}

// OpenOptions configures a read-only engine.
type OpenOptions struct {
	// MemoryLimit bounds DuckDB's buffer pool, e.g. "256MB".
	MemoryLimit string
	// Threads bounds query parallelism.
	Threads int
}

// OpenReadOnly opens a frozen DuckDB file read-only. External file and network
// access is disabled at the connection level as defense in depth; the SQL
// policy remains the primary boundary.
func OpenReadOnly(ctx context.Context, path string, opts OpenOptions) (*Engine, error) {
	threads := opts.Threads
	if threads <= 0 {
		threads = 2
	}
	mem := opts.MemoryLimit
	if mem == "" {
		mem = "256MB"
	}

	e := &Engine{path: path}
	var initErr error
	var mu sync.Mutex
	connector, err := duckdb.NewConnector(
		fmt.Sprintf("%s?access_mode=read_only&threads=%d", path, threads),
		func(ex driver.ExecerContext) error {
			for _, stmt := range []string{
				fmt.Sprintf("SET memory_limit='%s'", mem),
				"SET enable_external_access=false",
				"SET allow_community_extensions=false",
				"SET autoinstall_known_extensions=false",
				"SET autoload_known_extensions=false",
			} {
				if _, err := ex.ExecContext(ctx, stmt, nil); err != nil {
					mu.Lock()
					e.configNote = append(e.configNote, fmt.Sprintf("%s: %v", stmt, err))
					if strings.Contains(stmt, "enable_external_access") {
						initErr = fmt.Errorf("analysis: cannot disable external access: %w", err)
					}
					mu.Unlock()
				}
			}
			return nil
		},
	)
	if err != nil {
		return nil, fmt.Errorf("analysis: open connector %s: %w", path, err)
	}
	if initErr != nil {
		_ = connector.Close()
		return nil, initErr
	}

	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("analysis: ping %s: %w", path, err)
	}
	e.db = db

	var v string
	if err := e.db.QueryRowContext(ctx, "SELECT value FROM schema_meta WHERE key = 'schema_version'").Scan(&v); err == nil {
		e.schema = v
	}
	return e, nil
}

// SchemaVersion returns the frozen schema version ("" if unknown).
func (e *Engine) SchemaVersion() string { return e.schema }

// Path returns the on-disk path of the restored engine.
func (e *Engine) Path() string { return e.path }

// ConfigNotes lists optional DuckDB hardening statements that failed.
func (e *Engine) ConfigNotes() []string { return append([]string(nil), e.configNote...) }

// Close releases the engine.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil
	}
	e.closed = true
	return e.db.Close()
}

// Query validates and runs a single constrained SQL statement.
func (e *Engine) Query(ctx context.Context, policy Policy, req QueryRequest) (*QueryResult, error) {
	maxRows := req.MaxRows
	if maxRows <= 0 || maxRows > policy.MaxRows {
		maxRows = policy.MaxRows
	}
	runtimePolicy := policy
	runtimePolicy.MaxRows = maxRows

	safe, err := runtimePolicy.Validate(req.SQL)
	if err != nil {
		return nil, err
	}
	return e.run(ctx, safe, maxRows, policy)
}

// run executes already-validated SQL and converts rows to JSON-safe values.
func (e *Engine) run(ctx context.Context, sqlText string, maxRows int, policy Policy) (*QueryResult, error) {
	rows, err := e.db.QueryContext(ctx, sqlText)
	if err != nil {
		return nil, fmt.Errorf("analysis: query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("analysis: columns: %w", err)
	}
	if len(cols) > policy.MaxColumns {
		return nil, policyErr("sql_too_many_columns", fmt.Sprintf("query projects %d columns, max is %d", len(cols), policy.MaxColumns))
	}
	colTypes, _ := rows.ColumnTypes()
	out := &QueryResult{
		Columns: make([]Column, len(cols)),
		Limit:   maxRows,
	}
	for i, c := range cols {
		dbt := ""
		if i < len(colTypes) && colTypes[i] != nil {
			dbt = colTypes[i].DatabaseTypeName()
		}
		out.Columns[i] = Column{Name: c, Type: dbt}
	}

	scan := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range scan {
		ptrs[i] = &scan[i]
	}

	totalBytes := 0
	for rows.Next() {
		if len(out.Rows) >= maxRows {
			out.Truncated = true
			break
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("analysis: scan: %w", err)
		}
		row := make([]any, len(cols))
		for i, v := range scan {
			converted, truncated := jsonValue(v, policy.MaxValueBytes)
			if truncated {
				out.Notes = appendUnique(out.Notes, "some string/blob values were truncated")
			}
			row[i] = converted
			totalBytes += approxBytes(converted)
		}
		out.Rows = append(out.Rows, row)
		if totalBytes > policy.MaxResultBytes {
			out.Truncated = true
			out.Notes = appendUnique(out.Notes, "result byte budget reached")
			break
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("analysis: rows: %w", err)
	}
	out.RowCount = len(out.Rows)
	out.Bytes = totalBytes
	return out, nil
}

// jsonValue converts a driver value into a JSON-safe representation.
func jsonValue(v any, maxBytes int) (any, bool) {
	switch t := v.(type) {
	case nil:
		return nil, false
	case int64:
		if t > maxSafeJSONInt || t < -maxSafeJSONInt {
			return strconv.FormatInt(t, 10), false
		}
		return t, false
	case int32:
		return int64(t), false
	case int16:
		return int64(t), false
	case int8:
		return int64(t), false
	case int:
		return int64(t), false
	case uint64:
		if t > uint64(maxSafeJSONInt) {
			return strconv.FormatUint(t, 10), false
		}
		return int64(t), false
	case uint32:
		return int64(t), false
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return strconv.FormatFloat(t, 'g', -1, 64), false
		}
		return t, false
	case float32:
		return float64(t), false
	case bool:
		return t, false
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano), false
	case []byte:
		return fmt.Sprintf("<blob %d bytes>", len(t)), false
	case string:
		if len(t) > maxBytes {
			return t[:maxBytes] + "…<truncated>", true
		}
		return t, false
	default:
		return fmt.Sprint(v), false
	}
}

func approxBytes(v any) int {
	switch t := v.(type) {
	case nil:
		return 4
	case string:
		return len(t) + 2
	case bool:
		return 5
	default:
		return 8
	}
}

func appendUnique(list []string, v string) []string {
	for _, x := range list {
		if x == v {
			return list
		}
	}
	return append(list, v)
}
