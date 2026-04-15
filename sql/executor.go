package sql

import (
	"fmt"
	"log"
	"strings"

	"lns.com/minidb/catalog"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
)

// Result types.
type (
	SelectResult struct {
		Columns []string
		Rows    [][]interface{}
	}
	OKResult struct {
		AffectedRows int
		InsertID     int64
	}
)

// Executor executes SQL statements against the storage engine.
type Executor struct {
	engine *storage.StorageEngine
	mgr    *txn.Manager
	cat    *catalog.Catalog
	dbName string   // current database
	txn    *txn.Txn // active transaction (nil = autocommit)
	ts     *txn.TimestampOracle
}

func NewExecutor(engine *storage.StorageEngine, mgr *txn.Manager, cat *catalog.Catalog, dbName string) *Executor {
	return &Executor{
		engine: engine,
		mgr:    mgr,
		cat:    cat,
		dbName: dbName,
		ts:     txn.NewTimestampOracle(),
	}
}

// SetDatabase changes the current database.
func (e *Executor) SetDatabase(db string) {
	e.dbName = db
}

// Database returns the current database name.
func (e *Executor) Database() string {
	return e.dbName
}

// Execute parses and executes a SQL statement.
func (e *Executor) Execute(sql string) (interface{}, error) {
	p := NewParser()
	stmt, err := p.Parse(sql)
	if err != nil {
		return nil, err
	}
	return e.executeStmt(stmt)
}

// ExecuteStmt executes a pre-parsed statement.
func (e *Executor) ExecuteStmt(stmt Stmt) (interface{}, error) {
	return e.executeStmt(stmt)
}

func (e *Executor) executeStmt(stmt Stmt) (interface{}, error) {
	switch s := stmt.(type) {
	case *CreateDatabaseStmt:
		return e.execCreateDatabase(s)
	case *DropDatabaseStmt:
		return e.execDropDatabase(s)
	case *UseStmt:
		return e.execUse(s)
	case *CreateTableStmt:
		return e.execCreateTable(s)
	case *DropTableStmt:
		return e.execDropTable(s)
	case *ShowTablesStmt:
		return e.execShowTables()
	case *ShowDatabasesStmt:
		return e.execShowDatabases()
	case *InsertStmt:
		return e.execDML(func(txn *txn.Txn) (interface{}, error) {
			return e.execInsert(txn, s)
		})
	case *SelectStmt:
		return e.execDMLRead(func(txn *txn.Txn) (interface{}, error) {
			return e.execSelect(txn, s)
		})
	case *UpdateStmt:
		return e.execDML(func(txn *txn.Txn) (interface{}, error) {
			return e.execUpdate(txn, s)
		})
	case *DeleteStmt:
		return e.execDML(func(txn *txn.Txn) (interface{}, error) {
			return e.execDelete(txn, s)
		})
	case *BeginStmt:
		return e.execBegin()
	case *CommitStmt:
		return e.execCommit()
	case *RollbackStmt:
		return e.execRollback()
	default:
		return nil, fmt.Errorf("unsupported statement: %T", stmt)
	}
}

// --- DDL ---

func (e *Executor) execCreateDatabase(s *CreateDatabaseStmt) (interface{}, error) {
	if err := e.cat.CreateDatabase(s.Name); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

func (e *Executor) execDropDatabase(s *DropDatabaseStmt) (interface{}, error) {
	return &OKResult{}, e.cat.DropDatabase(s.Name)
}

func (e *Executor) execUse(s *UseStmt) (interface{}, error) {
	e.dbName = s.DBName
	return &OKResult{}, nil
}

func (e *Executor) execCreateTable(s *CreateTableStmt) (interface{}, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	cols := make([]storage.ColumnDef, len(s.Columns))
	pkCols := []int{}
	for i, cd := range s.Columns {
		cols[i] = storage.ColumnDef{
			Name:      cd.Name,
			Type:      colTypeFromString(cd.Type),
			Length:    cd.Length,
			Precision: cd.Precision,
			Scale:     cd.Scale,
			Nullable:  cd.Nullable,
			AutoInc:   cd.AutoInc,
		}
		if cd.Primary {
			pkCols = append(pkCols, i)
		}
	}
	// Default to first column as PK if none specified.
	if len(pkCols) == 0 {
		pkCols = []int{0}
	}

	td := &catalog.TableDef{
		Database: e.dbName,
		Name:     s.Table,
		Columns:  cols,
		PKCols:   pkCols,
	}

	// Create the data tree.
	treeKey := td.DataFile()
	if err := e.engine.OpenTree(treeKey); err != nil {
		return nil, err
	}

	if err := e.cat.CreateTable(td); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

func (e *Executor) execDropTable(s *DropTableStmt) (interface{}, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	return &OKResult{}, e.cat.DropTable(e.dbName, s.Table)
}

func (e *Executor) execShowTables() (interface{}, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	tables, err := e.cat.ListTables(e.dbName)
	if err != nil {
		return nil, err
	}
	result := &SelectResult{Columns: []string{"Tables"}}
	for _, t := range tables {
		result.Rows = append(result.Rows, []interface{}{t})
	}
	return result, nil
}

func (e *Executor) execShowDatabases() (interface{}, error) {
	dbs, err := e.cat.ListDatabases()
	if err != nil {
		return nil, err
	}
	result := &SelectResult{Columns: []string{"Database"}}
	for _, db := range dbs {
		result.Rows = append(result.Rows, []interface{}{db})
	}
	return result, nil
}

// --- DML ---

func (e *Executor) execDML(fn func(*txn.Txn) (interface{}, error)) (interface{}, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	autocommit := e.txn == nil
	var txn *txn.Txn
	if autocommit {
		txn = e.mgr.Begin()
		defer func() {
			if txn != nil {
				txn.Rollback()
			}
		}()
	} else {
		txn = e.txn
	}

	result, err := fn(txn)
	if err != nil {
		if autocommit {
			txn.Rollback()
			txn = nil
		}
		return nil, err
	}

	if autocommit {
		if err := txn.Commit(); err != nil {
			txn = nil
			return nil, err
		}
		txn = nil
	}
	return result, nil
}

func (e *Executor) execDMLRead(fn func(*txn.Txn) (interface{}, error)) (interface{}, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	if e.txn != nil {
		return fn(e.txn)
	}
	txn := e.mgr.Begin()
	defer txn.Rollback()
	return fn(txn)
}

func (e *Executor) execInsert(t *txn.Txn, s *InsertStmt) (interface{}, error) {
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()
	var lastID int64

	for _, rowVals := range s.Values {
		if len(rowVals) != len(td.Columns) {
			return nil, fmt.Errorf("column count mismatch: got %d, expected %d", len(rowVals), len(td.Columns))
		}

		// Coerce values.
		coerced := make([]interface{}, len(td.Columns))
		for i, val := range rowVals {
			c, err := storage.CoerceValue(td.Columns[i], val)
			if err != nil {
				return nil, fmt.Errorf("column %q: %w", td.Columns[i].Name, err)
			}
			// Handle auto-increment.
			if td.Columns[i].AutoInc && c == nil {
				id, err := e.cat.NextAutoInc(e.dbName, s.Table, td.Columns[i].Name)
				if err != nil {
					return nil, err
				}
				c = id
				lastID = id
			}
			coerced[i] = c
		}

		pkCols := td.PrimaryKeyColumns()
		pkVals := make([]interface{}, len(pkCols))
		for i, colIdx := range td.PKCols {
			pkVals[i] = coerced[colIdx]
		}

		pk := storage.EncodePrimaryKey(pkCols, pkVals...)
		rowData := storage.EncodeRow(td.Columns, coerced)

		if err := t.Insert(treeKey, pk, rowData); err != nil {
			return nil, err
		}
	}

	return &OKResult{AffectedRows: len(s.Values), InsertID: lastID}, nil
}

func (e *Executor) execSelect(t *txn.Txn, s *SelectStmt) (interface{}, error) {
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()
	log.Printf("execSelect db=%s table=%s treeKey=%s dbName=%q", e.dbName, s.Table, treeKey, e.dbName)

	// Determine output columns.
	var colIndices []int
	var colNames []string
	if s.SelectAll {
		colNames = make([]string, len(td.Columns))
		colIndices = make([]int, len(td.Columns))
		for i, col := range td.Columns {
			colNames[i] = col.Name
			colIndices[i] = i
		}
	} else {
		colIndices = make([]int, len(s.Columns))
		colNames = s.Columns
		for i, name := range s.Columns {
			idx := td.ColumnIndex(name)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column %q", name)
			}
			colIndices[i] = idx
		}
	}

	// Determine scan range.
	var start, end []byte
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
	}
	if start == nil {
		// Full table scan.
		start = []byte{0x00}
		end = []byte{0xFF}
	}

	var rows [][]interface{}
	pkCols := td.PrimaryKeyColumns()

	t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)

		// Apply WHERE filter (for conditions beyond PK).
		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}

		row := make([]interface{}, len(colIndices))
		for i, ci := range colIndices {
			row[i] = vals[ci]
		}
		rows = append(rows, row)
		return true
	})

	// Apply ORDER BY.
	if len(s.OrderBy) > 0 {
		e.sortRows(rows, colNames, s.OrderBy)
	}

	// Apply LIMIT.
	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}

	return &SelectResult{Columns: colNames, Rows: rows}, nil
}

func (e *Executor) execUpdate(t *txn.Txn, s *UpdateStmt) (interface{}, error) {
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()
	pkCols := td.PrimaryKeyColumns()
	affected := 0

	// Build set clause: map column name -> new value expr.
	setMap := make(map[int]Expr)
	for _, sc := range s.SetClauses {
		idx := td.ColumnIndex(sc.Column)
		if idx < 0 {
			return nil, fmt.Errorf("unknown column %q", sc.Column)
		}
		setMap[idx] = sc.Value
	}

	start, end := []byte{0x00}, []byte{0xFF}
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
	}

	t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)

		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}

		// Apply SET clauses.
		newVals := make([]interface{}, len(vals))
		copy(newVals, vals)
		for ci, expr := range setMap {
			v := e.evalExpr(td, expr, vals)
			coerced, err := storage.CoerceValue(td.Columns[ci], v)
			if err == nil {
				newVals[ci] = coerced
			} else {
				newVals[ci] = v
			}
		}

		newRow := storage.EncodeRow(td.Columns, newVals)
		if err := t.Update(treeKey, pkCols, pk, newRow); err != nil {
			return false
		}
		affected++
		return true
	})

	return &OKResult{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(t *txn.Txn, s *DeleteStmt) (interface{}, error) {
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()
	pkCols := td.PrimaryKeyColumns()
	affected := 0

	start, end := []byte{0x00}, []byte{0xFF}
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
	}

	t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)

		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}

		if err := t.Delete(treeKey, pkCols, pk); err != nil {
			return false
		}
		affected++
		return true
	})

	return &OKResult{AffectedRows: affected}, nil
}

// --- Transaction ---

func (e *Executor) execBegin() (interface{}, error) {
	if e.txn != nil {
		return nil, fmt.Errorf("transaction already active")
	}
	e.txn = e.mgr.Begin()
	return &OKResult{}, nil
}

func (e *Executor) execCommit() (interface{}, error) {
	if e.txn == nil {
		// No active transaction — return OK (compatible with MySQL behavior).
		return &OKResult{}, nil
	}
	err := e.txn.Commit()
	e.txn = nil
	if err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

func (e *Executor) execRollback() (interface{}, error) {
	if e.txn == nil {
		return &OKResult{}, nil
	}
	e.txn.Rollback()
	e.txn = nil
	return &OKResult{}, nil
}

// ActiveTxn returns the active transaction (for protocol layer).
func (e *Executor) ActiveTxn() *txn.Txn {
	return e.txn
}

// --- Expression evaluation ---

func (e *Executor) evalWhere(td *catalog.TableDef, where Expr, vals []interface{}) bool {
	result := e.evalExpr(td, where, vals)
	if b, ok := result.(bool); ok {
		return b
	}
	if result == nil {
		return false
	}
	return true
}

func (e *Executor) evalExpr(td *catalog.TableDef, expr Expr, vals []interface{}) interface{} {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		idx := td.ColumnIndex(ex.Name)
		if idx < 0 {
			return nil
		}
		return vals[idx]
	case *BinaryExpr:
		left := e.evalExpr(td, ex.Left, vals)
		right := e.evalExpr(td, ex.Right, vals)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExpr(td, ex.Operand, vals)
		return e.evalUnaryOp(ex.Op, operand)
	case *NullExpr:
		return nil
	case *IsNullExpr:
		v := e.evalExpr(td, ex.Expr, vals)
		if ex.Not {
			return v != nil
		}
		return v == nil
	default:
		return nil
	}
}

func (e *Executor) evalBinaryOp(op string, left, right interface{}) interface{} {
	switch op {
	case "=":
		return compareValues(left, right) == 0
	case "!=":
		return compareValues(left, right) != 0
	case "<":
		return compareValues(left, right) < 0
	case "<=":
		return compareValues(left, right) <= 0
	case ">":
		return compareValues(left, right) > 0
	case ">=":
		return compareValues(left, right) >= 0
	case "AND":
		return toBool(left) && toBool(right)
	case "OR":
		return toBool(left) || toBool(right)
	case "+":
		return arithOp(left, right, func(a, b int64) int64 { return a + b }, func(a, b float64) float64 { return a + b })
	case "-":
		return arithOp(left, right, func(a, b int64) int64 { return a - b }, func(a, b float64) float64 { return a - b })
	case "*":
		return arithOp(left, right, func(a, b int64) int64 { return a * b }, func(a, b float64) float64 { return a * b })
	default:
		return nil
	}
}

func (e *Executor) evalUnaryOp(op string, operand interface{}) interface{} {
	switch op {
	case "-":
		switch v := operand.(type) {
		case int32:
			return -v
		case int64:
			return -v
		case float64:
			return -v
		}
	case "NOT":
		return !toBool(operand)
	}
	return nil
}

// extractPKRange tries to extract a PK range from a WHERE clause.
// Returns nil start/end if it can't optimize.
func (e *Executor) extractPKRange(td *catalog.TableDef, where Expr) ([]byte, []byte) {
	// Collect all PK column equalities from the WHERE clause.
	eqMap := e.collectEqualities(where)

	// Build a prefix range from consecutive PK columns.
	var prefix []byte
	for _, colIdx := range td.PKCols {
		col := td.Columns[colIdx]
		val, ok := eqMap[col.Name]
		if !ok {
			break
		}
		coerced, err := storage.CoerceValue(col, val)
		if err != nil {
			break
		}
		prefix = append(prefix, storage.EncodeColumnValue(col, coerced)...)
	}
	if len(prefix) == 0 {
		return nil, nil
	}

	// Range: [prefix, prefix + 0xFF)
	end := make([]byte, len(prefix)+1)
	copy(end, prefix)
	end[len(prefix)] = 0xFF
	return prefix, end
}

// collectEqualities extracts col=val pairs from AND-connected equalities.
func (e *Executor) collectEqualities(expr Expr) map[string]interface{} {
	result := make(map[string]interface{})
	e.collectEqualitiesInto(expr, result)
	return result
}

func (e *Executor) collectEqualitiesInto(expr Expr, m map[string]interface{}) {
	binExpr, ok := expr.(*BinaryExpr)
	if !ok {
		return
	}
	if binExpr.Op == "AND" {
		e.collectEqualitiesInto(binExpr.Left, m)
		e.collectEqualitiesInto(binExpr.Right, m)
		return
	}
	if binExpr.Op != "=" {
		return
	}
	var colName string
	var val interface{}
	if col, ok := binExpr.Left.(*ColumnRefExpr); ok {
		colName = col.Name
		val = e.extractLiteral(binExpr.Right)
	} else if col, ok := binExpr.Right.(*ColumnRefExpr); ok {
		colName = col.Name
		val = e.extractLiteral(binExpr.Left)
	}
	if colName != "" && val != nil {
		m[colName] = val
	}
}

func (e *Executor) extractLiteral(expr Expr) interface{} {
	if lit, ok := expr.(*LiteralExpr); ok {
		return lit.Value
	}
	return nil
}

func (e *Executor) sortRows(rows [][]interface{}, colNames []string, orderBy []OrderByClause) {
	if len(orderBy) == 0 || len(rows) <= 1 {
		return
	}
	// Build column index map.
	colIdx := make(map[string]int)
	for i, name := range colNames {
		colIdx[name] = i
	}

	// Simple insertion sort (fine for small result sets).
	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			if e.lessThan(rows[j], rows[j-1], orderBy, colIdx) {
				rows[j], rows[j-1] = rows[j-1], rows[j]
			} else {
				break
			}
		}
	}
}

func (e *Executor) lessThan(a, b []interface{}, orderBy []OrderByClause, colIdx map[string]int) bool {
	for _, ob := range orderBy {
		idx := colIdx[ob.Column]
		cmp := compareValues(a[idx], b[idx])
		if cmp == 0 {
			continue
		}
		if ob.Desc {
			return cmp > 0
		}
		return cmp < 0
	}
	return false
}

// --- Helpers ---

func colTypeFromString(s string) storage.ColumnType {
	switch strings.ToUpper(s) {
	case "INT":
		return storage.ColTypeInt
	case "BIGINT":
		return storage.ColTypeBigInt
	case "VARCHAR":
		return storage.ColTypeVarchar
	case "DECIMAL":
		return storage.ColTypeDecimal
	case "TIMESTAMP", "DATETIME":
		return storage.ColTypeTimestamp
	case "DOUBLE", "FLOAT":
		return storage.ColTypeDouble
	default:
		return storage.ColTypeInt
	}
}

func compareValues(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int32:
		switch bv := b.(type) {
		case int32:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int64:
			av64 := int64(av)
			if av64 < bv {
				return -1
			}
			if av64 > bv {
				return 1
			}
			return 0
		default:
			return 0
		}
	case int64:
		switch bv := b.(type) {
		case int64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
		case int32:
			bv64 := int64(bv)
			if av < bv64 {
				return -1
			}
			if av > bv64 {
				return 1
			}
		}
		return 0
	case float64:
		bf, ok := b.(float64)
		if !ok {
			return 0
		}
		if av < bf {
			return -1
		}
		if av > bf {
			return 1
		}
		return 0
	case string:
		bs, ok := b.(string)
		if !ok {
			return 0
		}
		if av < bs {
			return -1
		}
		if av > bs {
			return 1
		}
		return 0
	}
	return 0
}

func toBool(v interface{}) bool {
	if v == nil {
		return false
	}
	switch b := v.(type) {
	case bool:
		return b
	case int32:
		return b != 0
	case int64:
		return b != 0
	}
	return false
}

func arithOp(a, b interface{}, intFn func(int64, int64) int64, floatFn func(float64, float64) float64) interface{} {
	switch av := a.(type) {
	case int32:
		switch bv := b.(type) {
		case int32:
			return intFn(int64(av), int64(bv))
		case int64:
			return intFn(int64(av), bv)
		}
	case int64:
		switch bv := b.(type) {
		case int32:
			return intFn(av, int64(bv))
		case int64:
			return intFn(av, bv)
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return floatFn(av, bv)
		}
	}
	return nil
}
