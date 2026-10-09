package analysis

import (
	"strings"
)

type tokenKind int

const (
	tokIdent tokenKind = iota
	tokNumber
	tokString
	tokPunct
)

type token struct {
	text       string // exact source text (strings include quotes)
	lower      string // lowercased, unquoted identifier (used for matching)
	kind       tokenKind
	start, end int
}

// tokenize splits SQL into tokens, dropping comments and preserving string
// literals and quoted identifiers as single tokens. It is intentionally small:
// the validator only needs statement boundaries, keywords, identifiers and
// parentheses.
func tokenize(src string) ([]token, error) {
	var toks []token
	i := 0
	n := len(src)
	for i < n {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' || c == '\v':
			i++
		case c == '-' && i+1 < n && src[i+1] == '-':
			i += 2
			for i < n && src[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < n && src[i+1] == '*':
			end := strings.Index(src[i+2:], "*/")
			if end < 0 {
				return nil, policyErr("sql_parse", "unterminated block comment")
			}
			i += 2 + end + 2
		case c == '\'':
			start := i
			i++
			for i < n {
				if src[i] == '\'' {
					if i+1 < n && src[i+1] == '\'' { // escaped quote
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			if i > n || (i == n && src[n-1] != '\'') {
				return nil, policyErr("sql_parse", "unterminated string literal")
			}
			toks = append(toks, token{text: src[start:i], lower: src[start:i], kind: tokString, start: start, end: i})
		case c == '"':
			start := i
			i++
			for i < n {
				if src[i] == '"' {
					if i+1 < n && src[i+1] == '"' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			if i > n || (i == n && src[n-1] != '"') {
				return nil, policyErr("sql_parse", "unterminated quoted identifier")
			}
			raw := src[start:i]
			toks = append(toks, token{text: raw, lower: strings.ToLower(raw[1 : len(raw)-1]), kind: tokIdent, start: start, end: i})
		case isIdentStart(c):
			start := i
			for i < n && isIdentPart(src[i]) {
				i++
			}
			raw := src[start:i]
			toks = append(toks, token{text: raw, lower: strings.ToLower(raw), kind: tokIdent, start: start, end: i})
		case c >= '0' && c <= '9':
			start := i
			for i < n && (isIdentPart(src[i]) || src[i] == '.') {
				i++
			}
			raw := src[start:i]
			toks = append(toks, token{text: raw, lower: strings.ToLower(raw), kind: tokNumber, start: start, end: i})
		default:
			// Maximal-munch operators so reconstructing with spaces cannot split
			// a meaningful operator (::, <=, >=, <>, !=, ||, ->>, ->).
			op := string(c)
			if i+1 < n {
				two := src[i : i+2]
				switch two {
				case "::", "<=", ">=", "<>", "!=", "||", "->", "=>", ":=":
					op = two
				}
				if op == "->" && i+2 < n && src[i+2] == '>' {
					op = "->>"
				}
			}
			toks = append(toks, token{text: op, lower: op, kind: tokPunct, start: i, end: i + len(op)})
			i += len(op)
		}
	}
	return toks, nil
}

func isIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c >= 0x80
}

func isIdentPart(c byte) bool {
	return isIdentStart(c) || (c >= '0' && c <= '9') || c == '$'
}

// Validate checks a single SQL statement and returns an equivalent statement
// with a hard outer LIMIT applied. The original statement text is preserved
// exactly (comments and trailing semicolons removed) so string literals and
// operators keep their meaning.
func (p Policy) Validate(query string) (string, error) {
	if len(query) > p.MaxSQLBytes {
		return "", policyErr("sql_too_large", "query exceeds the maximum allowed length")
	}
	toks, err := tokenize(query)
	if err != nil {
		return "", err
	}
	if len(toks) == 0 {
		return "", policyErr("sql_empty", "query is empty")
	}

	statements := splitStatements(toks)
	if len(statements) == 0 {
		return "", policyErr("sql_empty", "query is empty")
	}
	if len(statements) > 1 {
		return "", policyErr("sql_multi_statement", "only a single SQL statement is allowed")
	}
	stmt := statements[0]

	first := stmt[0]
	if first.kind != tokIdent || (first.lower != "select" && first.lower != "with") {
		return "", policyErr("sql_not_select", "only SELECT (or WITH ... SELECT) queries are allowed")
	}

	// Bare occurrence of a forbidden keyword anywhere in the statement.
	for _, t := range stmt {
		if t.kind != tokIdent {
			continue
		}
		if _, bad := forbiddenKeywords[t.lower]; bad {
			return "", policyErr("sql_forbidden_keyword", "query contains the forbidden keyword "+strings.ToUpper(t.text))
		}
	}

	cteNames, err := collectCTEs(stmt)
	if err != nil {
		return "", err
	}
	if err := p.checkTables(stmt, cteNames); err != nil {
		return "", err
	}
	if err := p.checkFunctions(stmt); err != nil {
		return "", err
	}

	normalized := strings.TrimSpace(query[toks[0].start:stmt[len(stmt)-1].end])
	normalized = strings.TrimSuffix(normalized, ";")
	normalized = strings.TrimSpace(normalized)

	return "SELECT * FROM ( " + normalized + " ) AS _burstel_sub LIMIT " + itoa(p.MaxRows), nil
}

// splitStatements groups tokens into statements on top-level ';'. Empty
// statements (e.g. a trailing semicolon) are dropped.
func splitStatements(toks []token) [][]token {
	var out [][]token
	var cur []token
	for _, t := range toks {
		if t.kind == tokPunct && t.text == ";" {
			if len(cur) > 0 {
				out = append(out, cur)
				cur = nil
			}
			continue
		}
		cur = append(cur, t)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return out
}

// collectCTEs returns the set of CTE names defined by a leading WITH clause.
func collectCTEs(stmt []token) (map[string]struct{}, error) {
	names := map[string]struct{}{}
	if len(stmt) == 0 || stmt[0].lower != "with" {
		return names, nil
	}
	// Reject RECURSIVE explicitly; recursive CTEs are not needed for analysis.
	for _, t := range stmt {
		if t.kind == tokIdent && t.lower == "recursive" {
			return nil, policyErr("sql_recursive", "recursive CTEs are not allowed")
		}
	}
	for i := 0; i+2 < len(stmt); i++ {
		if stmt[i].kind == tokIdent && stmt[i+1].lower == "as" && stmt[i+2].text == "(" {
			names[stmt[i].lower] = struct{}{}
		}
	}
	return names, nil
}

// checkTables verifies every table referenced after FROM/JOIN is allowlisted
// (or a CTE name). Derived tables are rejected to keep the surface small.
func (p Policy) checkTables(stmt []token, cteNames map[string]struct{}) error {
	// First pass: find every FROM/JOIN and inspect the following token.
	for i := 0; i < len(stmt); i++ {
		t := stmt[i]
		if t.kind != tokIdent || (t.lower != "from" && t.lower != "join") {
			continue
		}
		j := i + 1
		if j >= len(stmt) {
			return policyErr("sql_parse", "dangling "+strings.ToUpper(t.text))
		}
		next := stmt[j]
		if next.kind == tokPunct && next.text == "(" {
			return policyErr("sql_derived_table", "subqueries in FROM are not allowed")
		}
		if next.kind != tokIdent {
			return policyErr("sql_parse", "expected a table name after "+strings.ToUpper(t.text))
		}
		name := next.lower
		if _, ok := p.Tables[name]; !ok {
			if _, cte := cteNames[name]; !cte {
				return policyErr("sql_table_not_allowed", "table "+next.text+" is not queryable")
			}
		}
	}

	// Second pass: comma table lists inside a FROM clause.
	for i := 0; i < len(stmt); i++ {
		if stmt[i].kind != tokIdent || stmt[i].lower != "from" {
			continue
		}
		inFrom := true
		for j := i + 1; j < len(stmt) && inFrom; j++ {
			t := stmt[j]
			if t.kind == tokIdent {
				switch t.lower {
				case "where", "group", "having", "order", "limit", "offset", "qualify", "window", "union", "except", "intersect", "returning", "into":
					inFrom = false
				}
			}
			if t.kind == tokPunct && (t.text == ";" || t.text == ")") {
				inFrom = false
			}
			if t.kind == tokPunct && t.text == "," {
				k := j + 1
				if k >= len(stmt) {
					return policyErr("sql_parse", "dangling comma in FROM")
				}
				n := stmt[k]
				if n.kind != tokIdent {
					return policyErr("sql_parse", "expected a table name after comma in FROM")
				}
				if _, ok := p.Tables[n.lower]; !ok {
					if _, cte := cteNames[n.lower]; !cte {
						return policyErr("sql_table_not_allowed", "table "+n.text+" is not queryable")
					}
				}
			}
		}
	}
	return nil
}

// checkFunctions rejects any identifier that is called like a function but is
// not on the allowlist. SQL keywords (IN, OVER, CAST, ...) are always allowed.
func (p Policy) checkFunctions(stmt []token) error {
	for i := 0; i+1 < len(stmt); i++ {
		t := stmt[i]
		if t.kind != tokIdent {
			continue
		}
		next := stmt[i+1]
		if !(next.kind == tokPunct && next.text == "(") {
			continue
		}
		if _, ok := sqlKeywords[t.lower]; ok {
			continue
		}
		if _, ok := p.Functions[t.lower]; ok {
			continue
		}
		return policyErr("sql_function_not_allowed", "function "+t.text+" is not allowed")
	}
	return nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
