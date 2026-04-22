// Sqllogictest runner for minidb
// Parses .test files in the Sqllogictest format and executes them against minidb
package main

import (
	"bufio"
	"crypto/md5"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"lns.com/minidb/catalog"
	mindbsql "lns.com/minidb/sql"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
	"lns.com/minidb/wal"
)

type testResult struct {
	file           string
	totalStmts     int
	totalQueries   int
	passedStmts    int
	passedQueries  int
	failedStmts    int
	failedQueries  int
	skippedStmts   int
	skippedQueries int
	errors         int
	parseErrors    int
	failures       []failureDetail
}

type failureDetail struct {
	lineNum  int
	kind     string // "statement" or "query"
	sql      string
	expected string
	got      string
	reason   string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: runner <test-files-or-dirs...>")
		fmt.Println("  runner ./sqllogictest/evidence/")
		fmt.Println("  runner ./sqllogictest/select1.test")
		os.Exit(1)
	}

	// Collect test files
	var testFiles []string
	for _, arg := range os.Args[1:] {
		info, err := os.Stat(arg)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}
		if info.IsDir() {
			filepath.Walk(arg, func(path string, fi os.FileInfo, err error) error {
				if err != nil {
					return nil
				}
				if !fi.IsDir() && (strings.HasSuffix(path, ".test") || strings.HasSuffix(path, ".slt")) {
					testFiles = append(testFiles, path)
				}
				return nil
			})
		} else {
			testFiles = append(testFiles, arg)
		}
	}

	sort.Strings(testFiles)

	fmt.Printf("Found %d test files\n\n", len(testFiles))

	total := testResult{}
	for _, tf := range testFiles {
		result := runTestFile(tf)
		total.totalStmts += result.totalStmts
		total.totalQueries += result.totalQueries
		total.passedStmts += result.passedStmts
		total.passedQueries += result.passedQueries
		total.failedStmts += result.failedStmts
		total.failedQueries += result.failedQueries
		total.skippedStmts += result.skippedStmts
		total.skippedQueries += result.skippedQueries
		total.errors += result.errors
		total.parseErrors += result.parseErrors
		total.failures = append(total.failures, result.failures...)

		status := "PASS"
		if result.failedStmts+result.failedQueries+result.errors > 0 {
			status = "FAIL"
		}
		fmt.Printf("[%s] %-60s  stmts:%d/%d  queries:%d/%d  skip:%d  err:%d  parse_err:%d\n",
			status, filepath.Base(tf),
			result.passedStmts, result.totalStmts,
			result.passedQueries, result.totalQueries,
			result.skippedStmts+result.skippedQueries,
			result.errors, result.parseErrors)
	}

	fmt.Println("\n========== SUMMARY ==========")
	fmt.Printf("Total files:     %d\n", len(testFiles))
	fmt.Printf("Statements:      %d passed / %d total (%d skipped, %d failed)\n",
		total.passedStmts, total.totalStmts, total.skippedStmts, total.failedStmts)
	fmt.Printf("Queries:         %d passed / %d total (%d skipped, %d failed)\n",
		total.passedQueries, total.totalQueries, total.skippedQueries, total.failedQueries)
	fmt.Printf("Runtime errors:  %d\n", total.errors)
	fmt.Printf("Parse errors:    %d\n", total.parseErrors)

	if len(total.failures) > 0 {
		fmt.Println("\n========== FAILURE DETAILS (first 200) ==========")
		shown := 0
		for _, f := range total.failures {
			if shown >= 200 {
				break
			}
			fmt.Printf("\n--- %s line %d ---\n", f.kind, f.lineNum)
			fmt.Printf("SQL:      %s\n", f.sql)
			fmt.Printf("Expected: %s\n", f.expected)
			fmt.Printf("Got:      %s\n", f.got)
			fmt.Printf("Reason:   %s\n", f.reason)
			shown++
		}
		if len(total.failures) > 200 {
			fmt.Printf("\n... and %d more failures\n", len(total.failures)-200)
		}
	}

	// Categorize failures
	if len(total.failures) > 0 {
		fmt.Println("\n========== FAILURE CATEGORIES ==========")
		categories := categorizeFailures(total.failures)
		for _, cat := range categories {
			fmt.Printf("  %-40s %d failures\n", cat.name, cat.count)
			if len(cat.examples) > 0 {
				fmt.Printf("    Example: %s\n", cat.examples[0])
			}
		}
	}
}

type failureCategory struct {
	name    string
	count   int
	examples []string
}

func categorizeFailures(failures []failureDetail) []failureCategory {
	cats := map[string]*failureCategory{}

	for _, f := range failures {
		cat := categorize(f.sql, f.reason)
		if _, ok := cats[cat]; !ok {
			cats[cat] = &failureCategory{name: cat}
		}
		cats[cat].count++
		if len(cats[cat].examples) < 3 {
			cats[cat].examples = append(cats[cat].examples, truncate(f.sql, 120))
		}
	}

	var result []failureCategory
	for _, c := range cats {
		result = append(result, *c)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].count > result[j].count
	})
	return result
}

func categorize(sql, reason string) string {
	upper := strings.ToUpper(sql)

	// Parse errors indicate missing SQL features
	if strings.Contains(reason, "parse error") || strings.Contains(reason, "syntax") {
		return categorizeBySQLFeature(upper, "parse error")
	}

	// Runtime errors
	if strings.Contains(reason, "not supported") || strings.Contains(reason, "unsupported") {
		return categorizeBySQLFeature(upper, "unsupported")
	}

	// Wrong result
	return categorizeBySQLFeature(upper, "wrong result")
}

func categorizeBySQLFeature(upper string, suffix string) string {
	switch {
	case strings.Contains(upper, "GROUP BY") || strings.Contains(upper, "HAVING"):
		return "GROUP BY / HAVING: " + suffix
	case strings.Contains(upper, "GROUP_CONCAT"):
		return "GROUP_CONCAT: " + suffix
	case strings.Contains(upper, "JOIN"):
		return "JOIN: " + suffix
	case strings.Contains(upper, "UNION"):
		return "UNION: " + suffix
	case strings.Contains(upper, "CASE") || strings.Contains(upper, "WHEN"):
		return "CASE WHEN: " + suffix
	case strings.Contains(upper, "CAST(") || strings.Contains(upper, "CAST ("):
		return "CAST: " + suffix
	case strings.Contains(upper, "COALESCE"):
		return "COALESCE: " + suffix
	case strings.Contains(upper, "IFNULL"):
		return "IFNULL: " + suffix
	case strings.Contains(upper, "NULLIF"):
		return "NULLIF: " + suffix
	case strings.Contains(upper, "LIKE"):
		return "LIKE: " + suffix
	case strings.Contains(upper, " BETWEEN "):
		return "BETWEEN: " + suffix
	case strings.Contains(upper, " IN ("):
		return "IN clause: " + suffix
	case strings.Contains(upper, " EXISTS"):
		return "EXISTS subquery: " + suffix
	case strings.Contains(upper, " NOT "):
		return "NOT operator: " + suffix
	case strings.Contains(upper, "COUNT(") || strings.Contains(upper, "SUM(") || strings.Contains(upper, "AVG(") || strings.Contains(upper, "MIN(") || strings.Contains(upper, "MAX("):
		return "Aggregate functions: " + suffix
	case strings.Contains(upper, "ABS(") || strings.Contains(upper, "ROUND(") || strings.Contains(upper, "UPPER(") || strings.Contains(upper, "LOWER(") || strings.Contains(upper, "LENGTH(") || strings.Contains(upper, "TYPEOF(") || strings.Contains(upper, "NULLIF(") || strings.Contains(upper, "IFNULL(") || strings.Contains(upper, "IIF("):
		return "Scalar functions: " + suffix
	case strings.Contains(upper, "DISTINCT"):
		return "DISTINCT: " + suffix
	case strings.Contains(upper, "ORDER BY"):
		return "ORDER BY: " + suffix
	case strings.Contains(upper, "LIMIT"):
		return "LIMIT: " + suffix
	case strings.Contains(upper, "OFFSET"):
		return "OFFSET: " + suffix
	case strings.Contains(upper, "CREATE VIEW"):
		return "CREATE VIEW: " + suffix
	case strings.Contains(upper, "CREATE TRIGGER"):
		return "CREATE TRIGGER: " + suffix
	case strings.Contains(upper, "CREATE INDEX"):
		return "CREATE INDEX: " + suffix
	case strings.Contains(upper, "DROP VIEW"):
		return "DROP VIEW: " + suffix
	case strings.Contains(upper, "DROP INDEX"):
		return "DROP INDEX: " + suffix
	case strings.Contains(upper, "DROP TABLE"):
		return "DROP TABLE: " + suffix
	case strings.Contains(upper, "ALTER TABLE"):
		return "ALTER TABLE: " + suffix
	case strings.Contains(upper, "REPLACE INTO") || strings.Contains(upper, "INSERT OR REPLACE"):
		return "REPLACE: " + suffix
	case strings.Contains(upper, "REINDEX"):
		return "REINDEX: " + suffix
	case strings.Contains(upper, "TRANSACTION") || strings.Contains(upper, "BEGIN") || strings.Contains(upper, "COMMIT"):
		return "Transaction control: " + suffix
	case strings.Contains(upper, "SELECT"):
		return "SELECT: " + suffix
	case strings.Contains(upper, "INSERT"):
		return "INSERT: " + suffix
	case strings.Contains(upper, "UPDATE"):
		return "UPDATE: " + suffix
	case strings.Contains(upper, "DELETE"):
		return "DELETE: " + suffix
	default:
		return "Other: " + suffix
	}
}

func truncate(s string, maxLen int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.TrimSpace(s)
	if len(s) > maxLen {
		return s[:maxLen] + "..."
	}
	return s
}

func runTestFile(path string) testResult {
	result := testResult{file: path}

	// Create a fresh database for each test file
	dir, err := os.MkdirTemp("", "slt_test_*")
	if err != nil {
		fmt.Printf("  ERROR creating temp dir: %v\n", err)
		return result
	}
	defer os.RemoveAll(dir)

	// Open database components
	w, err := wal.Open(dir)
	if err != nil {
		fmt.Printf("  ERROR opening WAL: %v\n", err)
		return result
	}
	defer w.Close()

	engine, err := storage.OpenEngine(dir, 64, 4096)
	if err != nil {
		fmt.Printf("  ERROR opening engine: %v\n", err)
		return result
	}
	defer engine.Close()

	cat, err := catalog.Open(dir)
	if err != nil {
		fmt.Printf("  ERROR opening catalog: %v\n", err)
		return result
	}
	defer cat.Close()

	ts := txn.OpenTimestampOracle(dir)
	mgr := txn.NewManager(engine, ts, w, 1)
	defer mgr.Close()

	exec := mindbsql.NewExecutor(engine, mgr, cat, "")

	// Create and use a default database
	exec.Execute("CREATE DATABASE testdb")
	exec.SetDatabase("testdb")

	// Parse and execute test file
	f, err := os.Open(path)
	if err != nil {
		fmt.Printf("  ERROR opening test file: %v\n", err)
		return result
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	// Increase buffer size for large lines
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNum := 0
	skipNext := false
	skipAll := false

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// Skip comments
		if strings.HasPrefix(line, "#") {
			continue
		}

		// Handle blank lines
		if strings.TrimSpace(line) == "" {
			continue
		}

		// Handle skipif/onlyif
		if strings.HasPrefix(line, "skipif ") {
			// skipif minidb -> skip the next statement/query
			dbName := strings.TrimSpace(line[7:])
			if dbName == "minidb" {
				skipNext = true
			}
			continue
		}
		if strings.HasPrefix(line, "onlyif ") {
			dbName := strings.TrimSpace(line[7:])
			if dbName != "minidb" {
				skipNext = true
			}
			continue
		}

		// Handle halt
		if line == "halt" {
			break
		}
		if skipAll {
			continue
		}

		// Handle hash-threshold (ignore)
		if strings.HasPrefix(line, "hash-threshold") {
			continue
		}

		// Statement
		if strings.HasPrefix(line, "statement ") {
			expectOK := strings.Contains(line, "ok")
			sql := readStmtSQL(scanner, &lineNum)

			if skipNext {
				skipNext = false
				result.skippedStmts++
				result.totalStmts++
				continue
			}

			result.totalStmts++
			startLine := lineNum

			// Execute
			_, err := exec.Execute(sql)
			if expectOK {
				if err != nil {
					result.failedStmts++
					result.failures = append(result.failures, failureDetail{
						lineNum:  startLine,
						kind:     "statement",
						sql:      truncate(sql, 200),
						expected: "ok",
						got:      "error",
						reason:   err.Error(),
					})
				} else {
					result.passedStmts++
				}
			} else {
				// statement error - expect an error
				if err != nil {
					result.passedStmts++
				} else {
					result.failedStmts++
					result.failures = append(result.failures, failureDetail{
						lineNum:  startLine,
						kind:     "statement",
						sql:      truncate(sql, 200),
						expected: "error",
						got:      "ok",
						reason:   "expected error but statement succeeded",
					})
				}
			}
			continue
		}

		// Query
		if strings.HasPrefix(line, "query ") {
			parts := strings.Fields(line)
			// query <type-string> [<sort-mode>] [<label>]
			typeStr := ""
			sortMode := "nosort"
			if len(parts) >= 2 {
				typeStr = parts[1]
			}
			if len(parts) >= 3 {
				sortMode = parts[2]
			}

			sql, expectedLines := readQueryAndResults(scanner, &lineNum)
			startLine := lineNum

			if skipNext {
				skipNext = false
				result.skippedQueries++
				result.totalQueries++
				continue
			}

			result.totalQueries++

			// Execute query
			res, err := exec.Execute(sql)
			if err != nil {
				result.errors++
				result.failures = append(result.failures, failureDetail{
					lineNum:  startLine,
					kind:     "query",
					sql:      truncate(sql, 200),
					expected: strings.Join(expectedLines, "\n"),
					got:      "",
					reason:   "execution error: " + err.Error(),
				})
				continue
			}

			// Get actual results
			selRes, ok := res.(*mindbsql.SelectResult) //nolint
			if !ok {
				result.errors++
				result.failures = append(result.failures, failureDetail{
					lineNum:  startLine,
					kind:     "query",
					sql:      truncate(sql, 200),
					expected: strings.Join(expectedLines, "\n"),
					got:      fmt.Sprintf("not a select result: %T", res),
					reason:   "unexpected result type",
				})
				continue
			}

			actualLines := formatResult(selRes, typeStr, sortMode)

			// Compare
			expected := strings.Join(expectedLines, "\n")
			actual := strings.Join(actualLines, "\n")

			if matchExpected(expected, actual) {
				result.passedQueries++
			} else {
				result.failedQueries++
				result.failures = append(result.failures, failureDetail{
					lineNum:  startLine,
					kind:     "query",
					sql:      truncate(sql, 200),
					expected: expected,
					got:      actual,
					reason:   "result mismatch",
				})
			}
			continue
		}
	}

	return result
}

// readStmtSQL reads lines until a blank line. Used for "statement ok/error".
func readStmtSQL(scanner *bufio.Scanner, lineNum *int) string {
	var lines []string
	for scanner.Scan() {
		(*lineNum)++
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			break
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// readQueryAndResults reads the SQL lines (until ----) and then the expected
// result lines (until blank line). Returns the SQL and expected result lines.
func readQueryAndResults(scanner *bufio.Scanner, lineNum *int) (string, []string) {
	var sqlLines []string
	// Read SQL until we hit "----" or a blank line
	for scanner.Scan() {
		(*lineNum)++
		line := scanner.Text()
		trimmed := strings.TrimSpace(line)
		if trimmed == "----" {
			break
		}
		if trimmed == "" {
			// No expected results for this query
			return strings.Join(sqlLines, "\n"), nil
		}
		sqlLines = append(sqlLines, line)
	}

	// Read expected result lines until blank line
	var resultLines []string
	for scanner.Scan() {
		(*lineNum)++
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			break
		}
		resultLines = append(resultLines, line)
	}

	return strings.Join(sqlLines, "\n"), resultLines
}

// matchExpected checks if actual output matches the expected result.
// Handles both direct value comparison and "N values hashing to HASH" format.
func matchExpected(expected, actual string) bool {
	if expected == actual {
		return true
	}

	// Normalize both sides into flat value lists for comparison.
	// Expected format: one value per line (row-major order).
	// Actual format: tab-separated columns, newline-separated rows.
	expectedVals := splitValues(expected)
	actualVals := splitValues(actual)

	// Check for "N values hashing to HASH" format.
	joined := strings.Join(strings.Fields(expected), " ")
	if strings.Contains(joined, "values hashing to") {
		parts := strings.Fields(joined)
		if len(parts) >= 5 && parts[1] == "values" && parts[2] == "hashing" && parts[3] == "to" {
			expectedCount, err := strconv.Atoi(parts[0])
			if err == nil && expectedCount == len(actualVals) {
				expectedHash := parts[4]
				toHash := strings.Join(actualVals, "\n") + "\n"
				hash := fmt.Sprintf("%x", md5.Sum([]byte(toHash)))
				return hash == expectedHash
			}
		}
	}

	// Direct value comparison: normalize both to flat lists.
	if len(expectedVals) == len(actualVals) {
		match := true
		for i := range expectedVals {
			if expectedVals[i] != actualVals[i] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}

	return false
}

// splitValues splits a result string into individual values,
// handling both one-value-per-line and tab-separated formats.
func splitValues(s string) []string {
	var vals []string
	for _, line := range strings.Split(s, "\n") {
		for _, v := range strings.Split(line, "\t") {
			v = strings.TrimSpace(v)
			if v != "" {
				vals = append(vals, v)
			}
		}
	}
	return vals
}

func formatResult(res *mindbsql.SelectResult, typeStr string, sortMode string) []string {
	var lines []string

	for _, row := range res.Rows {
		var parts []string
		for i, val := range row {
			if i >= len(typeStr) {
				break
			}
			parts = append(parts, formatValue(val, typeStr[i]))
		}
		if len(parts) > 0 {
			lines = append(lines, strings.Join(parts, "\t"))
		}
	}

	// Apply sorting
	switch sortMode {
	case "rowsort":
		sort.Strings(lines)
	case "valuesort":
		// Split all values, sort them, re-group by number of columns
		var allValues []string
		for _, line := range lines {
			allValues = append(allValues, strings.Split(line, "\t")...)
		}
		sort.Strings(allValues)
		numCols := len(typeStr)
		if numCols > 0 {
			lines = nil
			for i := 0; i < len(allValues); i += numCols {
				end := i + numCols
				if end > len(allValues) {
					end = len(allValues)
				}
				lines = append(lines, strings.Join(allValues[i:end], "\t"))
			}
		}
	// "nosort" - keep as is
	}

	return lines
}

func formatValue(val any, typeChar byte) string {
	if val == nil {
		return "NULL"
	}
	switch typeChar {
	case 'I': // integer
		switch v := val.(type) {
		case int:
			return strconv.Itoa(v)
		case int64:
			return strconv.FormatInt(v, 10)
		case int32:
			return strconv.FormatInt(int64(v), 10)
		case float64:
			// If it's a whole number, format as int
			if v == float64(int64(v)) {
				return strconv.FormatInt(int64(v), 10)
			}
			return strconv.FormatFloat(v, 'f', -1, 64)
		case string:
			return v
		default:
			return fmt.Sprintf("%v", val)
		}
	case 'R': // real/float
		switch v := val.(type) {
		case float64:
			return formatFloat(v)
		case int:
			return formatFloat(float64(v))
		case int64:
			return formatFloat(float64(v))
		case string:
			return v
		default:
			return fmt.Sprintf("%v", val)
		}
	case 'T': // text
		switch v := val.(type) {
		case string:
			return v
		case []byte:
			return string(v)
		default:
			return fmt.Sprintf("%v", val)
		}
	default: // unknown type, just format
		return fmt.Sprintf("%v", val)
	}
}

func formatFloat(f float64) string {
	// Format like SQLite: if integer value, show as N.0
	if f == float64(int64(f)) && !isNaNInf(f) {
		return fmt.Sprintf("%.1f", f)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func isNaNInf(f float64) bool {
	return f != f || f > 1e308 || f < -1e308
}

func init() {
	// Suppress excessive logging from the database
	_ = time.Now()
}
