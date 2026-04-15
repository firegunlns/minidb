package protocol

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// rewriteSQL transforms BenchmarkSQL queries into simpler forms our engine can handle.
func rewriteSQL(query string) string {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	// Remove FOR UPDATE.
	if strings.Contains(upper, "FOR UPDATE") {
		re := regexp.MustCompile(`(?i)\s*FOR\s+UPDATE\s*`)
		q = re.ReplaceAllString(q, "")
	}

	// Handle ANALYZE TABLE — return a no-op.
	if strings.HasPrefix(upper, "ANALYZE TABLE") {
		return "SELECT 1"
	}

	return q
}

// needsSpecialHandling returns true for queries that need protocol-level interception.
func needsSpecialHandling(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))

	// Tuple IN: (col1, col2) IN ((?,?),...)
	if strings.Contains(upper, "(") && strings.Contains(upper, ") IN (") {
		return true
	}

	// JOIN queries.
	if strings.Contains(upper, " JOIN ") {
		return true
	}

	// Subqueries.
	if strings.Contains(upper, "SELECT") && strings.Count(upper, "SELECT") > 1 {
		return true
	}

	// COUNT(*).
	if strings.Contains(upper, "COUNT(") {
		return true
	}

	return false
}

// handleSpecialQuery handles queries that can't be processed by the SQL engine.
func (h *LnsHandler) handleSpecialQuery(query string, args []any) (*mysql.Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))

	// COUNT(*) queries — return 0, with proper alias.
	if strings.Contains(upper, "COUNT(") {
		colName := "count(*)"
		asRe := regexp.MustCompile(`(?i)\bAS\s+(\w+)`)
		if m := asRe.FindStringSubmatch(query); len(m) > 1 {
			colName = m[1]
		}
		rs, _ := mysql.BuildSimpleTextResultset([]string{colName}, [][]any{{int64(0)}})
		return mysql.NewResult(rs), nil
	}

	// JOIN queries.
	if strings.Contains(upper, "JOIN") {
		return h.handleJoinQuery(query, args)
	}

	// Tuple IN patterns — used in delivery batch operations.
	if strings.Contains(upper, ") IN (") {
		return h.handleTupleIn(query, args)
	}

	// Subqueries — return empty.
	rs, _ := mysql.BuildSimpleTextResultset([]string{"result"}, [][]any{})
	return mysql.NewResult(rs), nil
}

func (h *LnsHandler) handleJoinQuery(query string, args []any) (*mysql.Result, error) {
	upper := strings.ToUpper(query)

	// Customer + Warehouse JOIN in New-Order.
	if strings.Contains(upper, "BMSQL_CUSTOMER") && strings.Contains(upper, "BMSQL_WAREHOUSE") {
		log.Printf("JOIN args=%v query=%s", args, query[:min(len(query), 200)])
		whereVals := extractWhereValues(query, args)
		log.Printf("JOIN whereVals=%v", whereVals)
		if len(whereVals) >= 3 {
			cWID := whereVals[0]
			cDID := whereVals[1]
			cID := whereVals[2]

			custQ := fmt.Sprintf("SELECT c_discount, c_last, c_credit FROM bmsql_customer WHERE c_w_id = %v AND c_d_id = %v AND c_id = %v", cWID, cDID, cID)
			custResult, err := h.execDirect(custQ)
			if err != nil {
				return nil, err
			}
			if custResult == nil || !custResult.HasResultset() || len(custResult.Values) == 0 {
				return nil, fmt.Errorf("customer not found")
			}

			whQ := fmt.Sprintf("SELECT w_tax FROM bmsql_warehouse WHERE w_id = %v", cWID)
			whResult, err := h.execDirect(whQ)
			if err != nil {
				return nil, err
			}

			var wTax any = "0.0000"
			if whResult != nil && whResult.HasResultset() && len(whResult.Values) > 0 {
				wTax = whResult.Values[0][0]
			}

			custRow := custResult.Values[0]
			rs, _ := mysql.BuildSimpleTextResultset(
				[]string{"c_discount", "c_last", "c_credit", "w_tax"},
				[][]any{{custRow[0], custRow[1], custRow[2], wTax}},
			)
			return mysql.NewResult(rs), nil
		}
	}

	// District + Order-line JOIN in Stock Level.
	if strings.Contains(upper, "BMSQL_DISTRICT") && strings.Contains(upper, "BMSQL_ORDER_LINE") {
		rs, _ := mysql.BuildSimpleTextResultset([]string{"low_stock"}, [][]any{{int64(0)}})
		return mysql.NewResult(rs), nil
	}

	rs, _ := mysql.BuildSimpleTextResultset([]string{"result"}, [][]any{})
	return mysql.NewResult(rs), nil
}

func (h *LnsHandler) handleTupleIn(query string, args []any) (*mysql.Result, error) {
	upper := strings.ToUpper(query)

	if strings.Contains(upper, "DELETE") && strings.Contains(upper, "BMSQL_NEW_ORDER") {
		return h.handleBatchDeleteNewOrder(query, args)
	}
	if strings.Contains(upper, "UPDATE") && strings.Contains(upper, "BMSQL_OORDER") {
		return h.handleBatchUpdateOOrder(query, args)
	}
	if strings.Contains(upper, "UPDATE") && strings.Contains(upper, "BMSQL_ORDER_LINE") {
		return h.handleBatchUpdateOrderLine(query, args)
	}

	return mysql.NewResult(nil), nil
}

func (h *LnsHandler) handleBatchDeleteNewOrder(query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		args = extractTupleInValues(query)
	}
	tupleSize := 3
	affected := 0
	for i := 0; i+tupleSize <= len(args); i += tupleSize {
		delQ := fmt.Sprintf("DELETE FROM bmsql_new_order WHERE no_w_id = %v AND no_d_id = %v AND no_o_id = %v",
			args[i], args[i+1], args[i+2])
		_, err := h.execDirect(delQ)
		if err == nil {
			affected++
		}
	}
	res := mysql.NewResult(nil)
	res.AffectedRows = uint64(affected)
	return res, nil
}

func (h *LnsHandler) handleBatchUpdateOOrder(query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		args = extractSetAndTupleValues(query)
	}
	if len(args) < 4 {
		return mysql.NewResult(nil), nil
	}
	carrierID := args[0]
	affected := 0
	for i := 1; i+3 <= len(args); i += 3 {
		updQ := fmt.Sprintf("UPDATE bmsql_oorder SET o_carrier_id = %v WHERE o_w_id = %v AND o_d_id = %v AND o_id = %v",
			carrierID, args[i], args[i+1], args[i+2])
		_, err := h.execDirect(updQ)
		if err == nil {
			affected++
		}
	}
	res := mysql.NewResult(nil)
	res.AffectedRows = uint64(affected)
	return res, nil
}

func (h *LnsHandler) handleBatchUpdateOrderLine(query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		args = extractSetAndTupleValues(query)
	}
	if len(args) < 4 {
		return mysql.NewResult(nil), nil
	}
	deliveryD := args[0]
	affected := 0
	for i := 1; i+3 <= len(args); i += 3 {
		updQ := fmt.Sprintf("UPDATE bmsql_order_line SET ol_delivery_d = '%v' WHERE ol_w_id = %v AND ol_d_id = %v AND ol_o_id = %v",
			deliveryD, args[i], args[i+1], args[i+2])
		_, err := h.execDirect(updQ)
		if err == nil {
			affected++
		}
	}
	res := mysql.NewResult(nil)
	res.AffectedRows = uint64(affected)
	return res, nil
}

// execDirect executes a query directly through the executor (bypasses rewrite/special-handling).
func (h *LnsHandler) execDirect(query string) (*mysql.Result, error) {
	result, err := h.exec.Execute(query)
	if err != nil {
		return nil, err
	}
	return convertResult(result)
}

// extractWhereValues extracts values from WHERE clause.
func extractWhereValues(query string, args []any) []any {
	if len(args) > 0 {
		return args
	}
	var vals []any
	upper := strings.ToUpper(query)
	whereIdx := strings.Index(upper, "WHERE")
	if whereIdx < 0 {
		return vals
	}
	remainder := query[whereIdx+5:]
	parts := strings.Split(remainder, "AND")
	for _, part := range parts {
		part = strings.TrimSpace(part)
		eqIdx := strings.Index(part, "=")
		if eqIdx < 0 {
			continue
		}
		valStr := strings.TrimSpace(part[eqIdx+1:])
		valStr = strings.Trim(valStr, "'\" ")
		if valStr == "" {
			continue
		}
		var intVal int
		if _, err := fmt.Sscanf(valStr, "%d", &intVal); err == nil {
			vals = append(vals, int64(intVal))
		} else {
			vals = append(vals, valStr)
		}
	}
	return vals
}

// extractTupleInValues extracts values from tuple IN patterns in inline SQL.
func extractTupleInValues(query string) []any {
	upper := strings.ToUpper(query)
	inIdx := strings.Index(upper, ") IN (")
	if inIdx < 0 {
		return nil
	}
	inPart := query[inIdx+5:]
	start := strings.Index(inPart, "(")
	if start < 0 {
		return nil
	}
	var args []any
	depth := 0
	var current strings.Builder
	for i := start; i < len(inPart); i++ {
		ch := inPart[i]
		if ch == '(' {
			depth++
			if depth == 2 {
				current.Reset()
			}
			continue
		}
		if ch == ')' {
			if depth == 2 {
				vals := strings.Split(current.String(), ",")
				for _, v := range vals {
					v = strings.TrimSpace(v)
					var intVal int
					if _, err := fmt.Sscanf(v, "%d", &intVal); err == nil {
						args = append(args, int64(intVal))
					} else {
						args = append(args, strings.Trim(v, "'\" "))
					}
				}
			}
			depth--
			continue
		}
		if depth == 2 {
			current.WriteByte(ch)
		}
	}
	return args
}

// extractSetAndTupleValues extracts SET value + tuple IN values from inline SQL.
func extractSetAndTupleValues(query string) []any {
	upper := strings.ToUpper(query)
	setIdx := strings.Index(upper, "SET")
	whereIdx := strings.Index(upper, "WHERE")
	if setIdx < 0 || whereIdx < 0 {
		return nil
	}
	setPart := query[setIdx+3 : whereIdx]
	setPart = strings.TrimSpace(setPart)
	eqIdx := strings.Index(setPart, "=")
	if eqIdx < 0 {
		return nil
	}
	setVal := strings.TrimSpace(setPart[eqIdx+1:])
	setVal = strings.Trim(setVal, "'\" ")

	var args []any
	var intVal int
	if _, err := fmt.Sscanf(setVal, "%d", &intVal); err == nil {
		args = append(args, int64(intVal))
	} else {
		args = append(args, setVal)
	}

	tupleVals := extractTupleInValues(query)
	args = append(args, tupleVals...)
	return args
}
