// Package protocol 提供MySQL协议处理
// 兼容go-mysql库，实现MySQL服务器协议
package protocol

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"

	"lns.com/minidb/catalog"
	"lns.com/minidb/metrics"
	"lns.com/minidb/sql"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
)

// SvrHandler 实现go-mysql Handler接口
// 每个连接一个实例，无共享可变状态，无需互斥锁
// 每个连接的HandleCommand循环是单goroutine，顺序执行
type SvrHandler struct {
	exec       *sql.Executor          // SQL执行器
	engine     *storage.StorageEngine // 共享，只读
	mgr        *txn.Manager           // 共享，只读
	cat        *catalog.Catalog       // 共享，只读
	autocommit bool                   // 是否自动提交
}

func NewSvrHandler(engine *storage.StorageEngine, mgr *txn.Manager, cat *catalog.Catalog) *SvrHandler {
	return &SvrHandler{
		exec:       sql.NewExecutor(engine, mgr, cat, ""),
		engine:     engine,
		mgr:        mgr,
		cat:        cat,
		autocommit: true,
	}
}

func (h *SvrHandler) UseDB(dbName string) error {
	// Ensure the database exists; ignore "already exists" error.
	h.exec.Execute(fmt.Sprintf("CREATE DATABASE %s", dbName))
	h.exec.SetDatabase(dbName)
	return nil
}

func (h *SvrHandler) HandleQuery(query string) (result *mysql.Result, err error) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		metrics.QueryDuration.Observe(elapsed.Seconds())
		metrics.QueriesTotal.WithLabelValues("query").Inc()
		if elapsed > 5*time.Second {
			log.Printf("Slow query (%.1fs): %.200s", elapsed.Seconds(), query)
		}
		if r := recover(); r != nil {
			log.Printf("HandleQuery panic: %v", r)
			err = fmt.Errorf("internal error: %v", r)
		}
	}()

	q := rewriteSQL(query)
	upper := strings.ToUpper(strings.TrimSpace(q))

	if strings.HasPrefix(upper, "SET ") {
		h.handleSet(upper)
		return mysql.NewResult(nil), nil
	}

	h.autoBegin(upper)

	if strings.Contains(upper, "SELECT") && strings.Contains(upper, "@@") && !strings.Contains(upper, "FROM") {
		return handleSysVariable(q)
	}
	if upper == "SELECT LAST_INSERT_ID()" {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"LAST_INSERT_ID()"}, [][]any{{int64(0)}})
		return mysql.NewResult(rs), nil
	}

	if needsSpecialHandling(q) {
		return h.handleSpecialQuery(q, nil)
	}

	res, err := h.exec.Execute(q)
	if err != nil {
		return nil, err
	}
	return convertResult(res)
}

func (h *SvrHandler) HandleFieldList(table string, fieldWildcard string) ([]*mysql.Field, error) {
	return nil, nil
}

func (h *SvrHandler) HandleStmtPrepare(query string) (params int, columns int, context any, err error) {
	params = strings.Count(query, "?")
	upper := strings.ToUpper(query)
	if strings.Contains(upper, "SELECT") {
		columns = 1
	}
	return params, columns, query, nil
}

func (h *SvrHandler) HandleStmtExecute(context any, query string, args []any) (*mysql.Result, error) {
	start := time.Now()
	defer func() {
		metrics.QueryDuration.Observe(time.Since(start).Seconds())
		metrics.QueriesTotal.WithLabelValues("stmt_execute").Inc()
	}()
	actualQuery := replacePlaceholders(query, args)
	q := rewriteSQL(actualQuery)
	upper := strings.ToUpper(strings.TrimSpace(q))

	if strings.HasPrefix(upper, "SET ") {
		h.handleSet(upper)
		return mysql.NewResult(nil), nil
	}

	h.autoBegin(upper)

	if strings.Contains(upper, "SELECT") && strings.Contains(upper, "@@") && !strings.Contains(upper, "FROM") {
		return handleSysVariable(q)
	}
	if upper == "SELECT LAST_INSERT_ID()" {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"LAST_INSERT_ID()"}, [][]any{{int64(0)}})
		return mysql.NewResult(rs), nil
	}

	if needsSpecialHandling(q) {
		return h.handleSpecialQuery(q, args)
	}

	result, err := h.exec.Execute(q)
	if err != nil {
		return nil, err
	}
	return convertResult(result)
}

func (h *SvrHandler) HandleStmtClose(context any) error {
	return nil
}

func (h *SvrHandler) HandleOtherCommand(cmd byte, data []byte) error {
	return nil
}

func (h *SvrHandler) handleSet(upper string) {
	if strings.Contains(upper, "AUTOCOMMIT") {
		if strings.Contains(upper, "=0") {
			h.autocommit = false
		} else if strings.Contains(upper, "=1") {
			if !h.autocommit && h.exec.ActiveTxn() != nil {
				h.exec.Execute("COMMIT")
			}
			h.autocommit = true
		}
	}
}

func (h *SvrHandler) autoBegin(upper string) {
	if h.autocommit {
		return
	}
	trimmed := strings.TrimSpace(upper)
	if strings.HasPrefix(trimmed, "INSERT") || strings.HasPrefix(trimmed, "UPDATE") || strings.HasPrefix(trimmed, "DELETE") {
		if h.exec.ActiveTxn() == nil {
			h.exec.Execute("BEGIN")
		}
	}
}

func (h *SvrHandler) CloseConn() {
	if h.exec.ActiveTxn() != nil {
		h.exec.Execute("ROLLBACK")
	}
}

// --- System variable handling (stateless) ---

func handleSysVariable(query string) (*mysql.Result, error) {
	upper := strings.ToUpper(query)
	if strings.Count(upper, "@@") > 1 || strings.Contains(upper, " AS ") {
		return handleMultiSysVariable(query)
	}
	if strings.Contains(upper, "@@VERSION_COMMENT") {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"@@version_comment"}, [][]any{{"minidb"}})
		return mysql.NewResult(rs), nil
	}
	if strings.Contains(upper, "@@AUTOCOMMIT") {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"@@autocommit"}, [][]any{{int64(1)}})
		return mysql.NewResult(rs), nil
	}
	if strings.Contains(upper, "@@VERSION") {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"@@version"}, [][]any{{"8.0.0-minidb"}})
		return mysql.NewResult(rs), nil
	}
	rs, _ := mysql.BuildSimpleTextResultset([]string{"value"}, [][]any{{""}})
	return mysql.NewResult(rs), nil
}

func handleMultiSysVariable(query string) (*mysql.Result, error) {
	aliasRe := regexp.MustCompile(`(?i)AS\s+(\w+)`)
	aliasMatches := aliasRe.FindAllStringSubmatch(query, -1)
	defaults := map[string]string{
		"AUTO_INCREMENT_INCREMENT": "1",
		"CHARACTER_SET_CLIENT":     "utf8mb4",
		"CHARACTER_SET_CONNECTION": "utf8mb4",
		"CHARACTER_SET_RESULTS":    "utf8mb4",
		"CHARACTER_SET_SERVER":     "utf8mb4",
		"COLLATION_SERVER":         "utf8mb4_general_ci",
		"COLLATION_CONNECTION":     "utf8mb4_general_ci",
		"INIT_CONNECT":             "",
		"INTERACTIVE_TIMEOUT":      "28800",
		"LICENSE":                  "GPL",
		"LOWER_CASE_TABLE_NAMES":   "0",
		"MAX_ALLOWED_PACKET":       "67108864",
		"NET_WRITE_TIMEOUT":        "60",
		"PERFORMANCE_SCHEMA":       "0",
		"SQL_MODE":                 "",
		"SYSTEM_TIME_ZONE":         "UTC",
		"TIME_ZONE":                "SYSTEM",
		"TRANSACTION_ISOLATION":    "REPEATABLE-READ",
		"WAIT_TIMEOUT":             "28800",
		"VERSION":                  "8.0.0-minidb",
		"TX_ISOLATION":             "REPEATABLE-READ",
	}
	var cols []string
	var vals []any
	for _, m := range aliasMatches {
		alias := m[1]
		upperAlias := strings.ToUpper(alias)
		val := "0"
		if d, ok := defaults[upperAlias]; ok {
			val = d
		}
		cols = append(cols, alias)
		vals = append(vals, val)
	}
	if len(cols) == 0 {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"value"}, [][]any{{""}})
		return mysql.NewResult(rs), nil
	}
	rs, _ := mysql.BuildSimpleTextResultset(cols, [][]any{vals})
	return mysql.NewResult(rs), nil
}

// --- Result conversion (stateless) ---

func convertResult(result any) (*mysql.Result, error) {
	switch r := result.(type) {
	case *sql.SelectResult:
		return buildSelectResult(r)
	case *sql.OKResult:
		return buildOKResult(r)
	default:
		return mysql.NewResult(nil), nil
	}
}

func buildSelectResult(r *sql.SelectResult) (*mysql.Result, error) {
	values := make([][]any, len(r.Rows))
	for i, row := range r.Rows {
		vals := make([]any, len(row))
		for j, v := range row {
			vals[j] = convertValue(v)
		}
		values[i] = vals
	}
	rs, err := mysql.BuildSimpleTextResultset(r.Columns, values)
	if err != nil {
		return nil, err
	}
	return mysql.NewResult(rs), nil
}

func buildOKResult(r *sql.OKResult) (*mysql.Result, error) {
	res := mysql.NewResult(nil)
	res.AffectedRows = uint64(r.AffectedRows)
	res.InsertId = uint64(r.InsertID)
	return res, nil
}

func convertValue(v any) any {
	if v == nil {
		return nil
	}
	switch val := v.(type) {
	case int32:
		return int64(val)
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return val
	}
}

func replacePlaceholders(query string, args []any) string {
	idx := 0
	var buf strings.Builder
	for i := 0; i < len(query); i++ {
		if query[i] == '?' && idx < len(args) {
			buf.WriteString(formatValue(args[idx]))
			idx++
		} else {
			buf.WriteByte(query[i])
		}
	}
	return buf.String()
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", val)
	case int32:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case int:
		return fmt.Sprintf("%d", val)
	case uint32:
		return fmt.Sprintf("%d", val)
	case uint64:
		return fmt.Sprintf("%d", val)
	case float64:
		return fmt.Sprintf("%f", val)
	case bool:
		if val {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprintf("'%v'", val)
	}
}
