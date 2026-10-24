package compaction

import "strings"

// isMissingTableError reports whether a DuckDB error is "table does not exist".
// Analysis attaches both compacted segments (raw tables) and projection-only
// parts (spec 034), so a lookup must be able to fall back between the two
// layouts.
func isMissingTableError(err error) bool {
	if err == nil {
		return false
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "does not exist") || strings.Contains(text, "not found")
}
