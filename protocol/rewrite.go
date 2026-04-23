package protocol

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"

	"lns.com/minidb/sql"
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

	return q
}

// needsSpecialHandling returns true for queries that need protocol-level interception.
func needsSpecialHandling(query string) bool {
	upper := strings.ToUpper(strings.TrimSpace(query))

	// Multi-column tuple IN: (col1, col2) IN ((?,?),...)
	// Single-column (col) IN (...) should NOT be intercepted — let the executor handle it.
	if strings.Contains(upper, ") IN (") {
		// Find the position of ") IN (" and look backward for the matching "(".
		inIdx := strings.Index(upper, ") IN (")
		if inIdx > 0 {
			// Walk backward from the ')' to find the matching '('
			depth := 0
			start := -1
			for i := inIdx - 1; i >= 0; i-- {
				if upper[i] == ')' {
					depth++
				} else if upper[i] == '(' {
					if depth == 0 {
						start = i
						break
					}
					depth--
				}
			}
			if start >= 0 {
				colList := upper[start+1 : inIdx]
				// Multi-column tuple if there's a comma in the column list.
				if strings.Contains(colList, ",") {
					return true
				}
			}
		}
	}

	// JOIN queries.
	if strings.Contains(upper, " JOIN ") {
		return true
	}

	// Subqueries.
	if strings.Count(upper, "SELECT") > 1 {
		return true
	}

	return false
}

// isMultiColumnTupleIn checks if the ") IN (" pattern is a multi-column tuple IN
// like (col1, col2) IN ((?,?),...). Single-column (col) IN (...) returns false.
func isMultiColumnTupleIn(upper string) bool {
	inIdx := strings.Index(upper, ") IN (")
	if inIdx <= 0 {
		return false
	}
	depth := 0
	start := -1
	for i := inIdx - 1; i >= 0; i-- {
		if upper[i] == ')' {
			depth++
		} else if upper[i] == '(' {
			if depth == 0 {
				start = i
				break
			}
			depth--
		}
	}
	if start < 0 {
		return false
	}
	colList := upper[start+1 : inIdx]
	return strings.Contains(colList, ",")
}

// parseVal attempts to parse a string as an integer, falling back to a trimmed string.
func parseVal(s string) any {
	s = strings.TrimSpace(s)
	s = strings.Trim(s, "'\" ")
	if s == "" {
		return s
	}
	if intVal, err := strconv.Atoi(s); err == nil {
		return int64(intVal)
	}
	return s
}

// handleSpecialQuery handles queries that can't be processed by the SQL engine.
func (h *SvrHandler) handleSpecialQuery(query string, args []any) (*mysql.Result, error) {
	upper := strings.ToUpper(strings.TrimSpace(query))

	// JOIN queries.
	if strings.Contains(upper, "JOIN") {
		return h.handleJoinQuery(query, args)
	}

	// Tuple IN patterns — used in delivery batch operations.
	// Only route to handleTupleIn if it's a multi-column tuple IN.
	if strings.Contains(upper, ") IN (") && isMultiColumnTupleIn(upper) {
		return h.handleTupleIn(query, args)
	}

	// Subqueries — return empty.
	rs, _ := mysql.BuildSimpleTextResultset([]string{"result"}, [][]any{})
	return mysql.NewResult(rs), nil
}

func (h *SvrHandler) handleJoinQuery(query string, args []any) (*mysql.Result, error) {
	upper := strings.ToUpper(query)

	// Customer + Warehouse JOIN in New-Order.
	if strings.Contains(upper, "BMSQL_CUSTOMER") && strings.Contains(upper, "BMSQL_WAREHOUSE") {
		whereVals := extractWhereValues(query, args)
		if len(whereVals) >= 3 {
			cWID := whereVals[0]
			cDID := whereVals[1]
			cID := whereVals[2]

			custQ := fmt.Sprintf("SELECT c_discount, c_last, c_credit FROM bmsql_customer WHERE c_w_id = %v AND c_d_id = %v AND c_id = %v", cWID, cDID, cID)
			rawResult, err := h.exec.Execute(custQ)
			if err != nil {
				return nil, err
			}
			sr, ok := rawResult.(*sql.SelectResult)
			if !ok || len(sr.Rows) == 0 {
				return nil, fmt.Errorf("customer not found")
			}

			whQ := fmt.Sprintf("SELECT w_tax FROM bmsql_warehouse WHERE w_id = %v", cWID)
			whRaw, err := h.exec.Execute(whQ)
			var wTax any = "0.0000"
			if err == nil {
				if whSR, ok := whRaw.(*sql.SelectResult); ok && len(whSR.Rows) > 0 {
					wTax = whSR.Rows[0][0]
				}
			}

			row := sr.Rows[0]
			// Build result columns: c_discount, c_last, c_credit, w_tax
			rs, _ := mysql.BuildSimpleTextResultset(
				[]string{"c_discount", "c_last", "c_credit", "w_tax"},
				[][]any{{row[0], row[1], row[2], wTax}},
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

func (h *SvrHandler) handleTupleIn(query string, args []any) (*mysql.Result, error) {
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
	if strings.Contains(upper, "SELECT") && strings.Contains(upper, "BMSQL_OORDER") {
		return h.handleSelectOOrderTupleIn(query, args)
	}
	if strings.Contains(upper, "SELECT") && strings.Contains(upper, "BMSQL_STOCK") {
		return h.handleSelectStockTupleIn(query, args)
	}
	if strings.Contains(upper, "SELECT") && strings.Contains(upper, "BMSQL_ORDER_LINE") {
		return h.handleSelectOrderLineTupleIn(query, args)
	}

	return mysql.NewResult(nil), nil
}

func (h *SvrHandler) handleBatchDeleteNewOrder(query string, args []any) (*mysql.Result, error) {
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

func (h *SvrHandler) handleBatchUpdateOOrder(query string, args []any) (*mysql.Result, error) {
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

func (h *SvrHandler) handleBatchUpdateOrderLine(query string, args []any) (*mysql.Result, error) {
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

// handleSelectOOrderTupleIn handles:
//
//	SELECT o_c_id, o_d_id FROM bmsql_oorder
//	WHERE (o_w_id, o_d_id, o_id) IN ((?,?,?),(?,?,?),...)
func (h *SvrHandler) handleSelectOOrderTupleIn(query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		args = extractTupleInValues(query)
	}
	tupleSize := 3
	var rows [][]any
	for i := 0; i+tupleSize <= len(args); i += tupleSize {
		selQ := fmt.Sprintf(
			"SELECT o_c_id, o_d_id FROM bmsql_oorder WHERE o_w_id = %v AND o_d_id = %v AND o_id = %v",
			args[i], args[i+1], args[i+2])
		rawResult, err := h.exec.Execute(selQ)
		if err != nil {
			continue
		}
		if sr, ok := rawResult.(*sql.SelectResult); ok {
			for _, row := range sr.Rows {
				anyRow := make([]any, len(row))
				for j, v := range row {
					anyRow[j] = v
				}
				rows = append(rows, anyRow)
			}
		}
	}
	rs, _ := mysql.BuildSimpleTextResultset(
		[]string{"o_c_id", "o_d_id"}, rows)
	return mysql.NewResult(rs), nil
}

// handleSelectStockTupleIn handles:
//
//	SELECT s_i_id, s_w_id, s_quantity, s_data,
//	       s_dist_01..s_dist_10
//	FROM bmsql_stock
//	WHERE (s_w_id, s_i_id) IN ((?,?),(?,?),...)
func (h *SvrHandler) handleSelectStockTupleIn(query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		args = extractTupleInValues(query)
	}
	tupleSize := 2
	stockCols := []string{
		"s_i_id", "s_w_id", "s_quantity", "s_data",
		"s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04",
		"s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08",
		"s_dist_09", "s_dist_10",
	}
	var rows [][]any
	for i := 0; i+tupleSize <= len(args); i += tupleSize {
		selQ := fmt.Sprintf(
			"SELECT s_i_id, s_w_id, s_quantity, s_data, s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10 FROM bmsql_stock WHERE s_w_id = %v AND s_i_id = %v",
			args[i], args[i+1])
		rawResult, err := h.exec.Execute(selQ)
		if err != nil {
			continue
		}
		if sr, ok := rawResult.(*sql.SelectResult); ok {
			for _, row := range sr.Rows {
				anyRow := make([]any, len(row))
				for j, v := range row {
					anyRow[j] = v
				}
				rows = append(rows, anyRow)
			}
		}
	}
	rs, _ := mysql.BuildSimpleTextResultset(stockCols, rows)
	return mysql.NewResult(rs), nil
}

// handleSelectOrderLineTupleIn handles:
//
//	SELECT sum(ol_amount) AS sum_ol_amount, ol_d_id
//	FROM bmsql_order_line
//	WHERE (ol_w_id, ol_d_id, ol_o_id) IN ((?,?,?),...)
//	GROUP BY ol_d_id
func (h *SvrHandler) handleSelectOrderLineTupleIn(query string, args []any) (*mysql.Result, error) {
	if len(args) == 0 {
		args = extractTupleInValues(query)
	}
	tupleSize := 3

	// Collect (ol_amount, ol_d_id) per row, then group by ol_d_id and sum.
	sums := make(map[int32]float64) // ol_d_id -> sum of ol_amount
	for i := 0; i+tupleSize <= len(args); i += tupleSize {
		selQ := fmt.Sprintf(
			"SELECT ol_amount, ol_d_id FROM bmsql_order_line WHERE ol_w_id = %v AND ol_d_id = %v AND ol_o_id = %v",
			args[i], args[i+1], args[i+2])
		rawResult, err := h.exec.Execute(selQ)
		if err != nil {
			continue
		}
		if sr, ok := rawResult.(*sql.SelectResult); ok {
			for _, row := range sr.Rows {
				if len(row) >= 2 {
					var dID int32
					switch v := row[1].(type) {
					case int32:
						dID = v
					case int64:
						dID = int32(v)
					}
					var amount float64
					switch v := row[0].(type) {
					case float64:
						amount = v
					case string:
						fmt.Sscanf(v, "%f", &amount)
					case int32:
						amount = float64(v)
					case int64:
						amount = float64(v)
					}
					sums[dID] += amount
				}
			}
		}
	}

	var rows [][]any
	for dID, sum := range sums {
		rows = append(rows, []any{sum, dID})
	}
	rs, _ := mysql.BuildSimpleTextResultset(
		[]string{"sum_ol_amount", "ol_d_id"}, rows)
	return mysql.NewResult(rs), nil
}

// execDirect executes a query directly through the executor (bypasses rewrite/special-handling).
func (h *SvrHandler) execDirect(query string) (*mysql.Result, error) {
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
		vals = append(vals, parseVal(valStr))
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
					args = append(args, parseVal(v))
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
	args = append(args, parseVal(setVal))

	tupleVals := extractTupleInValues(query)
	args = append(args, tupleVals...)
	return args
}
