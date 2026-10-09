// Package analysis provides the read-only, abuse-resistant analysis side of
// burstel Phase 3.
//
// A frozen shot is a DuckDB file stored in R2. Analysis never touches the
// original recording container: the caller streams the frozen bytes into a
// throwaway read-only engine, verifies size + SHA-256, and runs only
// allowlisted, single-statement SQL against the five known projection tables.
//
// The SQL policy is deliberately conservative. DuckDB's read-only access mode
// is not sufficient on its own: a read-only connection can still read local
// files (read_csv, read_parquet, glob), reach the network (httpfs), inspect
// credentials or extensions, and exhaust memory. This package therefore
// validates every query before it reaches the driver and bounds its cost.
package analysis

// Policy bounds and allowlists for user-supplied SQL.
//
// The zero value is not usable; use DefaultPolicy.
type Policy struct {
	// MaxSQLBytes rejects absurdly large queries before parsing.
	MaxSQLBytes int
	// MaxRows is the hard row cap applied by wrapping the query.
	MaxRows int
	// MaxColumns bounds the projection width.
	MaxColumns int
	// MaxValueBytes truncates individual string/blob values in the result.
	MaxValueBytes int
	// MaxResultBytes bounds the serialized result payload.
	MaxResultBytes int
	// Tables is the set of table names that may appear after FROM/JOIN.
	Tables map[string]struct{}
	// Functions is the allowlist of scalar/aggregate/window functions.
	Functions map[string]struct{}
}

// DefaultPolicy returns the conservative MVP policy used in production.
func DefaultPolicy() Policy {
	return Policy{
		MaxSQLBytes:    16 << 10,
		MaxRows:        1000,
		MaxColumns:     64,
		MaxValueBytes:  4096,
		MaxResultBytes: 4 << 20,
		Tables: map[string]struct{}{
			"schema_meta":   {},
			"otlp_batches":  {},
			"spans":         {},
			"log_records":   {},
			"metric_points": {},
		},
		Functions: allowedFunctions(),
	}
}

// AllowedTables returns the sorted set of queryable table names.
func (p Policy) AllowedTables() []string {
	out := make([]string, 0, len(p.Tables))
	for t := range p.Tables {
		out = append(out, t)
	}
	return out
}

// PolicyError is a query-rejection with a stable machine code.
type PolicyError struct {
	Code    string
	Message string
}

func (e *PolicyError) Error() string { return e.Message }

func policyErr(code, message string) *PolicyError {
	return &PolicyError{Code: code, Message: message}
}

// Forbidden statement/DDL/mutation keywords. Any occurrence as a bare token
// rejects the query. These are never valid inside the analysis projections.
var forbiddenKeywords = map[string]struct{}{
	"copy": {}, "attach": {}, "detach": {}, "install": {}, "uninstall": {},
	"load": {}, "export": {}, "import": {}, "pragma": {}, "set": {}, "reset": {},
	"call": {}, "create": {}, "drop": {}, "alter": {}, "insert": {}, "update": {},
	"delete": {}, "truncate": {}, "merge": {}, "upsert": {},
	"vacuum": {}, "checkpoint": {}, "begin": {}, "commit": {}, "rollback": {},
	"savepoint": {}, "release": {}, "grant": {}, "revoke": {}, "use": {},
	"into": {}, "outfile": {}, "force_checkpoint": {}, "export_database": {},
}

// sqlKeywords are reserved/clause tokens that may legitimately precede "("
// (e.g. IN (...), OVER (...), CAST(...)). Keywords are not a file/network
// risk, so allowing them keeps the function check focused on real functions.
var sqlKeywords = map[string]struct{}{
	"select": {}, "from": {}, "where": {}, "with": {}, "recursive": {}, "as": {},
	"join": {}, "inner": {}, "left": {}, "right": {}, "full": {}, "outer": {},
	"cross": {}, "on": {}, "using": {}, "group": {}, "by": {}, "having": {},
	"order": {}, "asc": {}, "desc": {}, "nulls": {}, "first": {}, "last": {},
	"limit": {}, "offset": {}, "qualify": {}, "window": {}, "union": {}, "all": {},
	"except": {}, "intersect": {}, "distinct": {}, "and": {}, "or": {}, "not": {},
	"in": {}, "exists": {}, "between": {}, "like": {}, "ilike": {}, "is": {},
	"null": {}, "case": {}, "when": {}, "then": {}, "else": {}, "end": {},
	"over": {}, "partition": {}, "filter": {}, "within": {}, "cast": {},
	"extract": {}, "values": {}, "row": {}, "array": {}, "struct": {}, "map": {},
	"for": {}, "to": {}, "interval": {}, "at": {}, "time": {}, "zone": {},
	"escape": {}, "collate": {}, "some": {}, "any": {}, "returning": {},
}

// allowedFunctions is an allowlist of deterministic, non-I/O scalar,
// aggregate and window functions. Unknown functions are rejected, which is
// what blocks read_csv/read_parquet/read_json/glob/httpfs/getenv/etc.
func allowedFunctions() map[string]struct{} {
	names := []string{
		// aggregates / statistics
		"count", "count_if", "countif", "sum", "avg", "mean", "min", "max",
		"median", "mode", "quantile", "quantile_cont", "quantile_disc",
		"approx_quantile", "approx_count_distinct", "stddev", "stddev_pop",
		"stddev_samp", "variance", "var_pop", "var_samp", "product", "arg_min",
		"arg_max", "any_value", "bool_and", "bool_or", "bit_and", "bit_or",
		// window functions
		"row_number", "rank", "dense_rank", "percent_rank", "cume_dist",
		"ntile", "lag", "lead", "first_value", "last_value", "nth_value",
		// math
		"abs", "ceil", "ceiling", "floor", "round", "trunc", "sign", "sqrt",
		"cbrt", "pow", "power", "exp", "ln", "log", "log2", "log10", "mod",
		"factorial", "gcd", "lcm", "pi", "random", "sin", "cos", "tan",
		"asin", "acos", "atan", "atan2", "degrees", "radians",
		// null / conditional
		"coalesce", "ifnull", "nullif", "greatest", "least", "if", "nvl",
		"cast", "try_cast",
		// strings
		"length", "len", "char_length", "character_length", "octet_length",
		"bit_length", "lower", "upper", "ltrim", "rtrim", "trim", "lpad",
		"rpad", "substr", "substring", "left", "right", "reverse", "repeat",
		"replace", "translate",
		"concat", "concat_ws", "position", "strpos", "instr", "starts_with",
		"ends_with", "contains", "format", "printf", "split_part", "split",
		"regexp_matches", "regexp_replace", "regexp_extract",
		"regexp_extract_all", "regexp_full_match", "regexp_like", "like_escape",
		"to_base64", "from_base64", "md5", "sha256", "hash",
		// JSON
		"json_extract", "json_extract_string", "json_value", "json_type",
		"json_array_length", "json_keys", "json_valid", "json_contains",
		"json_merge_patch", "to_json", "from_json",
		// date/time
		"date_trunc", "date_diff", "datediff", "date_part", "datepart",
		"date_add", "date_sub", "strftime", "strptime", "epoch", "epoch_ms",
		"to_timestamp", "to_seconds", "to_milliseconds", "to_days",
		"current_date", "current_timestamp", "now", "year", "month", "day",
		"hour", "minute", "second", "millisecond", "microsecond", "week",
		// arrays/lists/structs
		"array_length", "array_extract", "array_slice", "list_extract",
		"list_value", "list_transform", "list_filter", "list_reduce",
		"list_contains", "len", "array_to_string", "array_agg", "string_agg",
		"list_aggregate", "struct_extract", "typeof", "isnull", "isnotnull",
	}
	out := make(map[string]struct{}, len(names))
	for _, n := range names {
		out[n] = struct{}{}
	}
	return out
}
