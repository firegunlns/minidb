package sql

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"lns.com/minidb/catalog"
	"lns.com/minidb/metrics"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
)

// evalError is used to propagate errors from expression evaluation
// back to the execution layer.
type evalError struct {
	err error
}

type (
	SelectResult struct {
		Columns    []string
		Rows       [][]any
		TableAlias string
	}
	RowIterator interface {
		Columns() []string
		Next() ([]any, error)
		Close() error
	}
	OKResult struct {
		AffectedRows int
		InsertID     int64
	}
)

type streamRowIterator struct {
	columns []string
	rows    <-chan []any
	errc    <-chan error
	cancel  context.CancelFunc
	done    <-chan struct{}
	closed  bool
	err     error
}

func (it *streamRowIterator) Columns() []string {
	return it.columns
}

func (it *streamRowIterator) Next() ([]any, error) {
	if it.err != nil {
		return nil, it.err
	}
	row, ok := <-it.rows
	if ok {
		return row, nil
	}
	if it.errc != nil {
		it.err = <-it.errc
	}
	return nil, it.err
}

func (it *streamRowIterator) Close() error {
	if it.closed {
		return it.err
	}
	it.closed = true
	if it.cancel != nil {
		it.cancel()
	}
	if it.done != nil {
		<-it.done
	}
	if it.err == nil && it.errc != nil {
		select {
		case it.err = <-it.errc:
		default:
		}
	}
	return it.err
}

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
func (e *Executor) Execute(sql string) (any, error) {
	sql = normalizeSQL(sql)

	parseStart := time.Now()
	p := NewParser()
	stmt, err := p.Parse(sql)
	metrics.ParseDuration.Observe(time.Since(parseStart).Seconds())
	if err != nil {
		return nil, err
	}

	execStart := time.Now()
	result, err := e.executeStmt(stmt)
	metrics.ExecuteDuration.WithLabelValues(stmtLabel(stmt)).Observe(time.Since(execStart).Seconds())
	return result, err
}

// ExecuteStream returns a pull-based iterator for simple SELECT statements.
// The boolean return is false when the statement should use Execute instead.
func (e *Executor) ExecuteStream(sql string) (RowIterator, bool, error) {
	sql = normalizeSQL(sql)

	p := NewParser()
	stmt, err := p.Parse(sql)
	if err != nil {
		return nil, false, err
	}
	s, ok := stmt.(*SelectStmt)
	if !ok {
		return nil, false, nil
	}

	switch ref := s.TableRef.(type) {
	case *SimpleTableRef:
		if !e.canStreamSimpleSelect(s, ref) {
			return nil, false, nil
		}
		if e.dbName == "" {
			return nil, false, fmt.Errorf("no database selected")
		}
		if e.txn != nil {
			iter, err := e.execSelectSimpleStream(e.txn, s, ref, nil)
			return iter, err == nil, err
		}
		t := e.mgr.Begin()
		cleanup := func() {
			t.Rollback()
		}
		iter, err := e.execSelectSimpleStream(t, s, ref, cleanup)
		if err != nil {
			t.Rollback()
			return nil, false, err
		}
		return iter, true, nil
	case *JoinTableRef:
		if !e.canStreamJoinSelect(s, ref) {
			return nil, false, nil
		}
		if e.dbName == "" {
			return nil, false, fmt.Errorf("no database selected")
		}
		if e.txn != nil {
			iter, err := e.execSelectJoinStream(e.txn, s, ref, nil)
			return iter, err == nil, err
		}
		t := e.mgr.Begin()
		cleanup := func() {
			t.Rollback()
		}
		iter, err := e.execSelectJoinStream(t, s, ref, cleanup)
		if err != nil {
			t.Rollback()
			return nil, false, err
		}
		return iter, true, nil
	default:
		return nil, false, nil
	}
}

func normalizeSQL(sql string) string {
	sql = rewriteEmptyInLists(sql)
	sql = rewriteDropIndex(sql)
	sql = rewriteCastInteger(sql)
	return rewriteFuncSpaces(sql)
}

// rewriteEmptyInLists replaces "IN ()" and "NOT IN ()" with equivalent expressions
// since the TiDB parser doesn't accept empty IN lists. Uses a sentinel string
// that is detected after parsing to handle the empty-list semantics correctly:
// "x IN ()" → false, "x NOT IN ()" → true (regardless of x, even NULL).
const emptyInSentinel = "__EMPTY_IN__"

func rewriteEmptyInLists(sql string) string {
	var buf strings.Builder
	i := 0
	inQuote := false
	for i < len(sql) {
		ch := sql[i]
		if ch == '\'' {
			if inQuote && i+1 < len(sql) && sql[i+1] == '\'' {
				buf.WriteString("''")
				i += 2
				continue
			}
			inQuote = !inQuote
			buf.WriteByte(ch)
			i++
			continue
		}
		if inQuote {
			buf.WriteByte(ch)
			i++
			continue
		}
		// Outside quotes: check for NOT IN () or IN ().
		upper := sql[i:]
		if len(upper) >= 10 && strings.EqualFold(upper[:10], "NOT IN ()") {
			buf.WriteString("NOT IN ('")
			buf.WriteString(emptyInSentinel)
			buf.WriteString("')")
			i += 10
			continue
		}
		if len(upper) >= 5 && strings.EqualFold(upper[:5], "IN ()") {
			buf.WriteString("IN ('")
			buf.WriteString(emptyInSentinel)
			buf.WriteString("')")
			i += 5
			continue
		}
		buf.WriteByte(ch)
		i++
	}
	return buf.String()
}

// rewriteDropIndex handles SQLite-style "DROP INDEX idx;" by adding "ON __lookup__" so
// the MySQL-style parser accepts it. The __lookup__ table name is detected after parsing
// to trigger a catalog-wide index search.
func rewriteDropIndex(sql string) string {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)

	// Match "DROP INDEX <name>;" or "DROP INDEX <name>" (no ON clause)
	if !strings.HasPrefix(upper, "DROP INDEX ") {
		return sql
	}
	// Check if it already has ON
	rest := trimmed[11:] // after "DROP INDEX "
	if strings.Contains(strings.ToUpper(rest), " ON ") {
		return sql
	}
	// Extract index name (remove trailing semicolon)
	idxName := strings.TrimSuffix(strings.TrimSpace(rest), ";")
	// Rewrite to MySQL syntax with a placeholder table name
	return "DROP INDEX " + idxName + " ON __lookup__"
}

// rewriteCastInteger rewrites "CAST(... AS INTEGER)" to "CAST(... AS SIGNED)"
// since TiDB's MySQL parser doesn't accept INTEGER as a cast target.
func rewriteCastInteger(sql string) string {
	// Simple regex-free approach: replace AS INTEGER) with AS SIGNED INTEGER)
	// We need to handle AS INTEGER, AS INT, AS BIGINT, AS SMALLINT, AS TINYINT
	// Also handle whitespace between the type name and closing paren.
	upper := sql
	replacements := []struct{ from, to string }{
		{" AS INTEGER)", " AS SIGNED)"},
		{" AS INTEGER )", " AS SIGNED )"},
		{" AS INT)", " AS SIGNED)"},
		{" AS INT )", " AS SIGNED )"},
		{" AS BIGINT)", " AS SIGNED)"},
		{" AS BIGINT )", " AS SIGNED )"},
		{" AS SMALLINT)", " AS SIGNED)"},
		{" AS SMALLINT )", " AS SIGNED )"},
		{" AS TINYINT)", " AS SIGNED)"},
		{" AS TINYINT )", " AS SIGNED )"},
		{" AS REAL)", " AS SIGNED)"},
		{" AS REAL )", " AS SIGNED )"},
	}
	// Case-insensitive replacement
	for _, r := range replacements {
		idx := 0
		for {
			pos := findCaseInsensitive(upper[idx:], r.from)
			if pos < 0 {
				break
			}
			actualPos := idx + pos
			sql = sql[:actualPos] + r.to + sql[actualPos+len(r.from):]
			upper = strings.ToUpper(sql)
			idx = actualPos + len(r.to)
		}
	}
	return sql
}

func findCaseInsensitive(s, target string) int {
	sUpper := strings.ToUpper(s)
	tUpper := strings.ToUpper(target)
	return strings.Index(sUpper, tUpper)
}

// rewriteFuncSpaces removes spaces between known function names and ( for
// TiDB parser compatibility. TiDB requires no space for CAST and aggregate
// functions: "SUM (...)" → "SUM(...)", "CAST (..." → "CAST(..."
func rewriteFuncSpaces(sql string) string {
	// Functions that require no space before ( in TiDB parser.
	funcs := []string{
		"CAST", "SUM", "COUNT", "MIN", "MAX", "AVG",
		"GROUP_CONCAT",
	}
	upper := strings.ToUpper(sql)
	for _, fn := range funcs {
		pattern := fn + " ("
		idx := 0
		for {
			pos := strings.Index(upper[idx:], pattern)
			if pos < 0 {
				break
			}
			actualPos := idx + pos
			// Make sure this is a word boundary (not part of a larger word).
			if actualPos > 0 {
				ch := upper[actualPos-1]
				if (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_' {
					idx = actualPos + len(fn)
					continue
				}
			}
			// Remove the space.
			sql = sql[:actualPos+len(fn)] + sql[actualPos+len(fn)+1:]
			upper = strings.ToUpper(sql)
			idx = actualPos + len(fn) + 1
		}
	}
	return sql
}

// ExecuteStmt executes a pre-parsed statement.
func (e *Executor) ExecuteStmt(stmt Stmt) (any, error) {
	return e.executeStmt(stmt)
}

func stmtLabel(stmt Stmt) string {
	switch stmt.(type) {
	case *InsertStmt:
		return "insert"
	case *SelectStmt:
		return "select"
	case *UpdateStmt:
		return "update"
	case *DeleteStmt:
		return "delete"
	case *BeginStmt, *CommitStmt, *RollbackStmt:
		return "txn"
	default:
		return "ddl"
	}
}

func (e *Executor) executeStmt(stmt Stmt) (any, error) {
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
	case *DescTableStmt:
		return e.execDescTable(s)
	case *ShowIndexStmt:
		return e.execShowIndex(s)
	case *InsertStmt:
		return e.execDML(func(txn *txn.Txn) (any, error) {
			return e.execInsert(txn, s)
		})
	case *SelectStmt:
		return e.execDMLRead(func(txn *txn.Txn) (any, error) {
			return e.execSelect(txn, s)
		})
	case *UpdateStmt:
		return e.execDML(func(txn *txn.Txn) (any, error) {
			return e.execUpdate(txn, s)
		})
	case *DeleteStmt:
		return e.execDML(func(txn *txn.Txn) (any, error) {
			return e.execDelete(txn, s)
		})
	case *BeginStmt:
		return e.execBegin()
	case *CommitStmt:
		return e.execCommit()
	case *RollbackStmt:
		return e.execRollback()
	case *AlterTableStmt:
		return e.execAlterTable(s)
	case *CreateIndexStmt:
		return e.execCreateIndex(s)
	case *DropIndexStmt:
		return e.execDropIndex(s)
	case *CreateViewStmt:
		return e.execCreateView(s)
	case *ExplainStmt:
		return e.execExplain(s)
	case *AnalyzeTableStmt:
		return e.execAnalyzeTable(s)
	case *SetOprStmt:
		return e.execDMLRead(func(txn *txn.Txn) (any, error) {
			return e.execSetOpr(txn, s)
		})
	default:
		return nil, fmt.Errorf("unsupported statement: %T", stmt)
	}
}

// --- DDL ---

func (e *Executor) execCreateDatabase(s *CreateDatabaseStmt) (any, error) {
	if err := e.cat.CreateDatabase(s.Name); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

func (e *Executor) execDropDatabase(s *DropDatabaseStmt) (any, error) {
	return &OKResult{}, e.cat.DropDatabase(s.Name)
}

func (e *Executor) execUse(s *UseStmt) (any, error) {
	e.dbName = s.DBName
	return &OKResult{}, nil
}

func (e *Executor) execCreateTable(s *CreateTableStmt) (any, error) {
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
	// When no PRIMARY KEY is specified, add a hidden _rowid column as PK
	// instead of using the first column. This prevents duplicate rows from
	// being overwritten when the first column has duplicate values.
	if len(pkCols) == 0 {
		rowidCol := storage.ColumnDef{
			Name:    "_rowid",
			Type:    storage.ColTypeInt,
			AutoInc: true,
			Hidden:  true,
		}
		cols = append(cols, rowidCol)
		pkCols = []int{len(cols) - 1}
	}

	td := &catalog.TableDef{
		Database: e.dbName,
		Name:     s.Table,
		Columns:  cols,
		PKCols:   pkCols,
	}

	// Copy inline index definitions.
	for _, idx := range s.Indexes {
		td.Indexes = append(td.Indexes, catalog.IndexDef{
			Name:    idx.Name,
			Columns: idx.Columns,
			Unique:  idx.Unique,
		})
	}

	treeKey := td.DataFile()
	if err := e.engine.OpenTree(treeKey); err != nil {
		return nil, err
	}

	// Open index tree files.
	for i := range td.Indexes {
		idxTreeKey := td.IndexFile(&td.Indexes[i])
		if err := e.engine.OpenTree(idxTreeKey); err != nil {
			return nil, err
		}
	}

	if err := e.cat.CreateTable(td); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

func (e *Executor) execCreateIndex(s *CreateIndexStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	// Validate columns exist.
	for _, colName := range s.Columns {
		if td.ColumnIndex(colName) < 0 {
			return nil, fmt.Errorf("column %q not found in table %q", colName, s.Table)
		}
	}

	idxDef := catalog.IndexDef{
		Name:    s.IndexName,
		Columns: s.Columns,
		Unique:  s.Unique,
	}

	// Create the index tree file.
	idxTreeKey := td.IndexFile(&idxDef)
	if err := e.engine.OpenTree(idxTreeKey); err != nil {
		return nil, err
	}

	// Backfill: scan existing data and populate index.
	pkCols := td.PrimaryKeyColumns()
	idxColDefs := idxColumnDefs(td, &idxDef)
	txn := e.mgr.Begin()
	txn.Scan(td.DataFile(), pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		idxVals := make([]any, len(idxDef.Columns))
		for j, colName := range idxDef.Columns {
			idxVals[j] = vals[td.ColumnIndex(colName)]
		}
		idxKey := storage.EncodeIndexKeyWithRawPK(idxColDefs, idxVals, pk)
		txn.Insert(idxTreeKey, idxKey, nil)
		return true
	})
	if err := txn.Commit(); err != nil {
		return nil, fmt.Errorf("index backfill failed: %w", err)
	}

	// Update catalog.
	td.Indexes = append(td.Indexes, idxDef)
	if err := e.cat.UpdateTable(e.dbName, s.Table, td); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

// idxColumnDefs returns the storage.ColumnDef slice for an index's columns.
func idxColumnDefs(td *catalog.TableDef, idx *catalog.IndexDef) []storage.ColumnDef {
	cols := make([]storage.ColumnDef, len(idx.Columns))
	for i, name := range idx.Columns {
		cols[i] = td.Columns[td.ColumnIndex(name)]
	}
	return cols
}

func (e *Executor) execAnalyzeTable(s *AnalyzeTableStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	stats := &catalog.TableStats{UpdateTime: time.Now().Unix()}
	treeKey := td.DataFile()
	start := []byte{0x00}
	end := []byte{0xFF}

	// Fast row count
	stats.RowCount = e.engine.CountAll(treeKey, start, end)

	// Per-column accumulators
	stats.ColStats = make([]catalog.ColumnStats, len(td.Columns))
	for i, col := range td.Columns {
		stats.ColStats[i] = catalog.ColumnStats{Name: col.Name}
	}
	colNDV := make([]map[string]bool, len(td.Columns))
	colNullCnt := make([]int64, len(td.Columns))
	colMin := make([]any, len(td.Columns))
	colMax := make([]any, len(td.Columns))
	colStrLen := make([]int64, len(td.Columns))
	for i := range colNDV {
		colNDV[i] = make(map[string]bool)
	}

	// Full scan
	e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		for i, val := range vals {
			if val == nil {
				colNullCnt[i]++
				continue
			}
			key := fmt.Sprintf("%v", val)
			colNDV[i][key] = true
			if colMin[i] == nil || compareValues(val, colMin[i]) < 0 {
				colMin[i] = val
			}
			if colMax[i] == nil || compareValues(val, colMax[i]) > 0 {
				colMax[i] = val
			}
			if s, ok := val.(string); ok {
				colStrLen[i] += int64(len(s))
			}
		}
		return true
	})

	// Finalize
	nonNull := stats.RowCount
	for i := range stats.ColStats {
		stats.ColStats[i].NDV = int64(len(colNDV[i]))
		stats.ColStats[i].NullCnt = colNullCnt[i]
		stats.ColStats[i].MinVal = colMin[i]
		stats.ColStats[i].MaxVal = colMax[i]
		if nonNull > colNullCnt[i] && colStrLen[i] > 0 {
			stats.ColStats[i].AvgLen = colStrLen[i] / (nonNull - colNullCnt[i])
		}
	}

	td.Stats = stats
	return &OKResult{}, e.cat.UpdateTable(e.dbName, s.Table, td)
}

func (e *Executor) execDropTable(s *DropTableStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	if s.IsView {
		err := e.cat.DropView(e.dbName, s.Table)
		if err != nil {
			if s.IfExists {
				return &OKResult{}, nil
			}
			return nil, err
		}
		return &OKResult{}, nil
	}
	err := e.cat.DropTable(e.dbName, s.Table)
	if err != nil {
		if s.IfExists {
			return &OKResult{}, nil
		}
		return nil, err
	}
	return &OKResult{}, nil
}

func (e *Executor) execDropIndex(s *DropIndexStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	if s.Table != "" {
		return nil, e.cat.DropIndexFromTable(e.dbName, s.Table, s.IndexName)
	}
	return nil, e.cat.DropIndex(e.dbName, s.IndexName)
}

func (e *Executor) execCreateView(s *CreateViewStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	// Serialize the SELECT statement back to SQL for storage.
	// We store the view's SELECT query so it can be re-parsed and executed when referenced.
	selectSQL := viewSelectToSQL(s.Select)
	if err := e.cat.CreateView(e.dbName, s.ViewName, selectSQL); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

// viewSelectToSQL reconstructs a SQL string from a SelectStmt for view storage.
func viewSelectToSQL(s *SelectStmt) string {
	var buf strings.Builder
	buf.WriteString("SELECT ")
	if s.SelectAll {
		buf.WriteString("*")
	} else {
		for i, f := range s.Fields {
			if i > 0 {
				buf.WriteString(", ")
			}
			if f.Expr != nil {
				buf.WriteString(exprToSQL(f.Expr))
			} else if f.Column != "" {
				buf.WriteString(f.Column)
			}
			if f.Alias != "" {
				buf.WriteString(" AS ")
				buf.WriteString(f.Alias)
			}
		}
	}
	if s.TableRef != nil {
		buf.WriteString(" FROM ")
		tableRefToSQL(&buf, s.TableRef)
	}
	if s.Where != nil {
		buf.WriteString(" WHERE ")
		buf.WriteString(exprToSQL(s.Where))
	}
	if len(s.OrderBy) > 0 {
		buf.WriteString(" ORDER BY ")
		for i, o := range s.OrderBy {
			if i > 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(exprToSQL(o.Expr))
			if o.Desc {
				buf.WriteString(" DESC")
			}
		}
	}
	if s.Limit != nil {
		buf.WriteString(" LIMIT ")
		buf.WriteString(strconv.Itoa(*s.Limit))
	}
	return buf.String()
}

func tableRefToSQL(buf *strings.Builder, ref TableRef) {
	switch r := ref.(type) {
	case *SimpleTableRef:
		buf.WriteString(r.Table)
		if r.Alias != "" {
			buf.WriteString(" AS ")
			buf.WriteString(r.Alias)
		}
	case *JoinTableRef:
		tableRefToSQL(buf, r.Left)
		switch r.Type {
		case JoinTypeCross:
			buf.WriteString(" JOIN ")
		case JoinTypeLeft:
			buf.WriteString(" LEFT JOIN ")
		case JoinTypeRight:
			buf.WriteString(" RIGHT JOIN ")
		}
		tableRefToSQL(buf, r.Right)
		if r.On != nil {
			buf.WriteString(" ON ")
			buf.WriteString(exprToSQL(r.On))
		}
	}
}

func exprToSQL(e Expr) string {
	switch v := e.(type) {
	case *LiteralExpr:
		if v.Value == nil {
			return "NULL"
		}
		switch val := v.Value.(type) {
		case string:
			return "'" + val + "'"
		default:
			return fmt.Sprintf("%v", val)
		}
	case *ColumnRefExpr:
		if v.Table != "" {
			return v.Table + "." + v.Name
		}
		return v.Name
	case *BinaryExpr:
		return exprToSQL(v.Left) + " " + v.Op + " " + exprToSQL(v.Right)
	case *UnaryExpr:
		return v.Op + " " + exprToSQL(v.Operand)
	case *IsNullExpr:
		return exprToSQL(v.Expr) + " IS NULL"
	case *InExpr:
		base := exprToSQL(v.Expr)
		if v.Not {
			base += " NOT IN ("
		} else {
			base += " IN ("
		}
		for i, val := range v.Values {
			if i > 0 {
				base += ", "
			}
			base += exprToSQL(val)
		}
		return base + ")"
	case *FuncCallExpr:
		name := v.Name
		name += "("
		for i, arg := range v.Args {
			if i > 0 {
				name += ", "
			}
			name += exprToSQL(arg)
		}
		return name + ")"
	case *SubqueryExpr:
		return "(" + viewSelectToSQL(v.Query) + ")"
	case *ExistsExpr:
		return "EXISTS (" + viewSelectToSQL(v.Query) + ")"
	case *BetweenExpr:
		return exprToSQL(v.Expr) + " BETWEEN " + exprToSQL(v.Low) + " AND " + exprToSQL(v.High)
	case *LikeExpr:
		return exprToSQL(v.Expr) + " LIKE " + exprToSQL(v.Pattern)
	case *CaseExpr:
		s := "CASE"
		if v.Value != nil {
			s += " " + exprToSQL(v.Value)
		}
		for _, w := range v.Whens {
			s += " WHEN " + exprToSQL(w.Cond) + " THEN " + exprToSQL(w.Result)
		}
		if v.Else != nil {
			s += " ELSE " + exprToSQL(v.Else)
		}
		return s + " END"
	default:
		return "?"
	}
}

func (e *Executor) execShowTables() (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	tables, err := e.cat.ListTables(e.dbName)
	if err != nil {
		return nil, err
	}
	result := &SelectResult{Columns: []string{"Tables"}}
	for _, t := range tables {
		result.Rows = append(result.Rows, []any{t})
	}
	return result, nil
}

func (e *Executor) execShowDatabases() (any, error) {
	dbs, err := e.cat.ListDatabases()
	if err != nil {
		return nil, err
	}
	result := &SelectResult{Columns: []string{"Database"}}
	for _, db := range dbs {
		result.Rows = append(result.Rows, []any{db})
	}
	return result, nil
}

func (e *Executor) execDescTable(s *DescTableStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}
	result := &SelectResult{
		Columns: []string{"Field", "Type", "Null", "Key", "Default", "Extra"},
	}
	for i, col := range td.Columns {
		isPK := false
		for _, pkIdx := range td.PKCols {
			if pkIdx == i {
				isPK = true
				break
			}
		}
		nullStr := "YES"
		if !col.Nullable {
			nullStr = "NO"
		}
		keyStr := ""
		if isPK {
			keyStr = "PRI"
		}
		defaultStr := ""
		extraStr := ""
		if col.AutoInc {
			extraStr = "auto_increment"
		}
		typeStr := columnTypeName(col.Type, col.Length, col.Precision, col.Scale)
		result.Rows = append(result.Rows, []any{
			col.Name,
			typeStr,
			nullStr,
			keyStr,
			defaultStr,
			extraStr,
		})
	}
	return result, nil
}

func (e *Executor) execShowIndex(s *ShowIndexStmt) (any, error) {
	dbName := e.dbName
	if s.DB != "" {
		dbName = s.DB
	}
	if dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	td, err := e.cat.GetTable(dbName, s.Table)
	if err != nil {
		return nil, err
	}

	result := &SelectResult{
		Columns: []string{"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name", "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment", "Visible", "Expression"},
	}

	// Primary key rows.
	for seq, pkIdx := range td.PKCols {
		col := td.Columns[pkIdx]
		nullStr := "YES"
		if !col.Nullable {
			nullStr = "NO"
		}
		result.Rows = append(result.Rows, []any{
			s.Table,
			int32(0), // Non_unique = 0 for PK
			"PRIMARY",
			int32(seq + 1),
			col.Name,
			"A",
			int64(0), // Cardinality (unknown)
			nil,      // Sub_part
			nil,      // Packed
			nullStr,
			"BTREE",
			"",
			"",
			"YES",
			nil,
		})
	}

	// Secondary index rows.
	for _, idx := range td.Indexes {
		for seq, colName := range idx.Columns {
			colIdx := td.ColumnIndex(colName)
			nullStr := "YES"
			if colIdx >= 0 && !td.Columns[colIdx].Nullable {
				nullStr = "NO"
			}
			nonUnique := int32(1)
			if idx.Unique {
				nonUnique = 0
			}
			result.Rows = append(result.Rows, []any{
				s.Table,
				nonUnique,
				idx.Name,
				int32(seq + 1),
				colName,
				"A",
				int64(0),
				nil,
				nil,
				nullStr,
				"BTREE",
				"",
				"",
				"YES",
				nil,
			})
		}
	}

	return result, nil
}

func (e *Executor) execAlterTable(s *AlterTableStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}

	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	for _, spec := range s.Specs {
		switch spec.Type {
		case AlterAddColumn:
			for _, col := range spec.Columns {
				storageCol := storage.ColumnDef{
					Name:      col.Name,
					Type:      colTypeFromString(col.Type),
					Length:    col.Length,
					Precision: col.Precision,
					Scale:     col.Scale,
					Nullable:  col.Nullable,
					AutoInc:   col.AutoInc,
				}
				td.Columns = append(td.Columns, storageCol)
			}
		case AlterDropColumn:
			colIdx := td.ColumnIndex(spec.Name)
			if colIdx < 0 {
				return nil, fmt.Errorf("column %q not found", spec.Name)
			}
			td.Columns = append(td.Columns[:colIdx], td.Columns[colIdx+1:]...)
			for i := range td.PKCols {
				if td.PKCols[i] == colIdx {
					td.PKCols = append(td.PKCols[:i], td.PKCols[i+1:]...)
					break
				}
			}
			for i := range td.PKCols {
				if td.PKCols[i] > colIdx {
					td.PKCols[i]--
				}
			}
			var newFKs []catalog.ForeignKeyDef
			for _, fk := range td.ForeignKeys {
				var newCols []int
				for _, idx := range fk.Columns {
					if idx != colIdx {
						if idx > colIdx {
							newCols = append(newCols, idx-1)
						} else {
							newCols = append(newCols, idx)
						}
					}
				}
				if len(newCols) > 0 {
					fk.Columns = newCols
					newFKs = append(newFKs, fk)
				}
			}
			td.ForeignKeys = newFKs
		case AlterAddConstraint:
			if spec.Constraint == nil {
				continue
			}
			c := spec.Constraint
			switch c.Type {
			case ConstraintTypePrimaryKey:
				var pkCols []int
				for _, colName := range c.Keys {
					idx := td.ColumnIndex(colName)
					if idx < 0 {
						return nil, fmt.Errorf("column %q not found for primary key", colName)
					}
					pkCols = append(pkCols, idx)
				}
				td.PKCols = pkCols
			case ConstraintTypeForeignKey:
				var colIndices []int
				for _, colName := range c.Keys {
					idx := td.ColumnIndex(colName)
					if idx < 0 {
						return nil, fmt.Errorf("column %q not found for foreign key", colName)
					}
					colIndices = append(colIndices, idx)
				}
				refTd, err := e.cat.GetTable(e.dbName, c.ReferTable)
				if err != nil {
					return nil, fmt.Errorf("referenced table %q not found: %w", c.ReferTable, err)
				}
				var refColIndices []int
				for _, colName := range c.ReferKeys {
					idx := refTd.ColumnIndex(colName)
					if idx < 0 {
						return nil, fmt.Errorf("referenced column %q not found in %s", colName, c.ReferTable)
					}
					refColIndices = append(refColIndices, idx)
				}
				fk := catalog.ForeignKeyDef{
					Name:       c.Name,
					Columns:    colIndices,
					RefTable:   c.ReferTable,
					RefColumns: refColIndices,
				}
				td.ForeignKeys = append(td.ForeignKeys, fk)
			case ConstraintTypeUnique:
			}
		case AlterDropConstraint:
			for i, idx := range td.Indexes {
				if idx.Name == spec.Name {
					td.Indexes = append(td.Indexes[:i], td.Indexes[i+1:]...)
					break
				}
			}
			for i, fk := range td.ForeignKeys {
				if fk.Name == spec.Name {
					td.ForeignKeys = append(td.ForeignKeys[:i], td.ForeignKeys[i+1:]...)
					break
				}
			}
		}
	}

	if err := e.cat.UpdateTable(e.dbName, s.Table, td); err != nil {
		return nil, err
	}
	return &OKResult{}, nil
}

func columnTypeName(ct storage.ColumnType, length, precision, scale int) string {
	switch ct {
	case storage.ColTypeInt:
		return "int"
	case storage.ColTypeBigInt:
		return "bigint"
	case storage.ColTypeVarchar:
		if length > 0 {
			return fmt.Sprintf("varchar(%d)", length)
		}
		return "varchar"
	case storage.ColTypeDecimal:
		if precision > 0 && scale > 0 {
			return fmt.Sprintf("decimal(%d,%d)", precision, scale)
		} else if precision > 0 {
			return fmt.Sprintf("decimal(%d)", precision)
		}
		return "decimal"
	case storage.ColTypeTimestamp:
		return "timestamp"
	case storage.ColTypeDouble:
		return "double"
	default:
		return "unknown"
	}
}

// --- DML ---

func (e *Executor) execDML(fn func(*txn.Txn) (any, error)) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ee, ok := r.(evalError); ok {
				err = ee.err
			} else {
				panic(r)
			}
		}
	}()
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

	result, err = fn(txn)
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

func (e *Executor) execDMLRead(fn func(*txn.Txn) (any, error)) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			if ee, ok := r.(evalError); ok {
				err = ee.err
			} else {
				panic(r)
			}
		}
	}()
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

func (e *Executor) execInsert(t *txn.Txn, s *InsertStmt) (any, error) {
	td, err := e.cat.GetTable(e.dbName, s.Table)
	if err != nil {
		return nil, err
	}

	// If INSERT ... SELECT, execute the SELECT first to get rows.
	if s.Select != nil {
		res, err := e.execSelect(t, s.Select)
		if err != nil {
			return nil, err
		}
		sr, ok := res.(*SelectResult)
		if !ok {
			return nil, fmt.Errorf("INSERT SELECT: unexpected result type")
		}
		// Convert SelectResult rows into Values format.
		for _, row := range sr.Rows {
			s.Values = append(s.Values, row)
		}
	}

	treeKey := td.DataFile()
	var lastID int64

	// Build column index map when explicit columns are specified.
	var colIndexMap map[string]int
	if len(s.Columns) > 0 {
		colIndexMap = make(map[string]int, len(s.Columns))
		for i, name := range s.Columns {
			colIndexMap[name] = i
		}
	}

	for _, rowVals := range s.Values {
		// When columns are explicitly listed, map provided values to
		// their positions in the table definition; missing columns get nil.
		var fullVals []any
		if colIndexMap != nil {
			fullVals = make([]any, len(td.Columns))
			for colIdx, col := range td.Columns {
				if srcIdx, ok := colIndexMap[col.Name]; ok && srcIdx < len(rowVals) {
					fullVals[colIdx] = rowVals[srcIdx]
				}
			}
		} else {
			// Count visible (non-hidden) columns for mismatch check.
			visibleCount := 0
			for _, col := range td.Columns {
				if !col.Hidden {
					visibleCount++
				}
			}
			if len(rowVals) != visibleCount && len(rowVals) != len(td.Columns) {
				return nil, fmt.Errorf("column count mismatch: got %d, expected %d", len(rowVals), visibleCount)
			}
			if len(rowVals) == visibleCount && visibleCount < len(td.Columns) {
				// Provided values for visible columns only; hidden columns get nil (auto-filled by AutoInc).
				fullVals = make([]any, len(td.Columns))
				visIdx := 0
				for colIdx, col := range td.Columns {
					if col.Hidden {
						continue
					}
					fullVals[colIdx] = rowVals[visIdx]
					visIdx++
				}
			} else {
				fullVals = rowVals
			}
		}

		// Coerce values.
		coerced := make([]any, len(td.Columns))
		for i, val := range fullVals {
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
				// Coerce auto-increment value to the column's native type.
				c, _ = storage.CoerceValue(td.Columns[i], id)
				lastID = id
			}
			coerced[i] = c
		}

		pkCols := td.PrimaryKeyColumns()
		pkVals := make([]any, len(pkCols))
		for i, colIdx := range td.PKCols {
			pkVals[i] = coerced[colIdx]
		}

		pk := storage.EncodePrimaryKey(pkCols, pkVals...)
		rowData := storage.EncodeRow(td.Columns, coerced)

		if err := t.Insert(treeKey, pk, rowData); err != nil {
			return nil, err
		}

		// Insert into secondary indexes.
		for i := range td.Indexes {
			idx := &td.Indexes[i]
			idxTreeKey := td.IndexFile(idx)
			idxColDefs := idxColumnDefs(td, idx)
			idxVals := make([]any, len(idx.Columns))
			for j, colName := range idx.Columns {
				idxVals[j] = coerced[td.ColumnIndex(colName)]
			}
			idxKey := storage.EncodeIndexKeyWithRawPK(idxColDefs, idxVals, pk)
			t.Insert(idxTreeKey, idxKey, nil)
		}
	}

	return &OKResult{AffectedRows: len(s.Values), InsertID: lastID}, nil
}

func (e *Executor) execSelect(t *txn.Txn, s *SelectStmt) (any, error) {
	if s.TableRef == nil {
		return e.execSelectNoTable(s)
	}
	switch ref := s.TableRef.(type) {
	case *SimpleTableRef:
		return e.execSelectSimple(t, s, ref)
	case *JoinTableRef:
		return e.execSelectJoin(t, s, ref)
	default:
		return nil, fmt.Errorf("unsupported table reference")
	}
}

// execSelectNoTable handles SELECT without a FROM clause (e.g., SELECT 1+2, SELECT MAX(1)).
func (e *Executor) execSelectNoTable(s *SelectStmt) (any, error) {
	// Evaluate each field expression without a table context.
	var row []any
	var colNames []string
	for _, f := range s.Fields {
		if f.Expr != nil {
			val := e.evalExprNoTable(f.Expr)
			row = append(row, val)
			name := f.Alias
			if name == "" {
				name = exprToString(f.Expr)
			}
			colNames = append(colNames, name)
		} else if f.Column != "" {
			row = append(row, nil)
			colNames = append(colNames, f.Column)
		}
	}

	rows := [][]any{row}

	// Handle DISTINCT
	if s.Distinct {
		rows = dedupRows(rows)
	}

	// Apply LIMIT
	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}

	return &SelectResult{Rows: rows, Columns: colNames}, nil
}

// dedupRows removes duplicate rows.
func dedupRows(rows [][]any) [][]any {
	seen := make(map[string]bool)
	var result [][]any
	for _, row := range rows {
		key := rowKey(row)
		if !seen[key] {
			seen[key] = true
			result = append(result, row)
		}
	}
	return result
}

func (e *Executor) execSetOpr(t *txn.Txn, s *SetOprStmt) (any, error) {
	if len(s.Selects) == 0 {
		return nil, fmt.Errorf("empty set operation")
	}

	// Execute first SELECT
	firstResult, err := e.execSelect(t, s.Selects[0].Select)
	if err != nil {
		return nil, err
	}
	rows := firstResult.(*SelectResult).Rows
	columns := firstResult.(*SelectResult).Columns

	// Apply subsequent set operations left to right
	for i := 1; i < len(s.Selects); i++ {
		nextResult, err := e.execSelect(t, s.Selects[i].Select)
		if err != nil {
			return nil, err
		}
		nextRows := nextResult.(*SelectResult).Rows
		rows = applySetOp(rows, nextRows, s.Selects[i].Opr)
	}

	// Apply ORDER BY
	if len(s.OrderBy) > 0 {
		rows = sortResultRows(rows, columns, s.OrderBy)
	}

	// Apply LIMIT
	if s.Limit != nil && len(rows) > *s.Limit {
		rows = rows[:*s.Limit]
	}

	return &SelectResult{Columns: columns, Rows: rows}, nil
}

func rowKey(row []any) string {
	var buf strings.Builder
	for i, v := range row {
		if i > 0 {
			buf.WriteByte(0)
		}
		switch v := v.(type) {
		case nil:
			buf.WriteByte('N')
		case bool:
			if v {
				buf.WriteByte('T')
			} else {
				buf.WriteByte('F')
			}
		case int32:
			buf.WriteByte('I')
			buf.WriteString(strconv.FormatInt(int64(v), 10))
		case int64:
			buf.WriteByte('I')
			buf.WriteString(strconv.FormatInt(v, 10))
		case float64:
			buf.WriteByte('F')
			buf.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
		case string:
			buf.WriteByte('S')
			buf.WriteString(v)
		default:
			buf.WriteByte('?')
			fmt.Fprintf(&buf, "%v", v)
		}
	}
	return buf.String()
}

func applySetOp(left, right [][]any, opr SetOprType) [][]any {
	switch opr {
	case SetOprUnion:
		return setUnionDistinct(left, right)
	case SetOprUnionAll:
		return append(left, right...)
	case SetOprExcept:
		return setExceptDistinct(left, right)
	case SetOprExceptAll:
		return setExceptAll(left, right)
	case SetOprIntersect:
		return setIntersectDistinct(left, right)
	case SetOprIntersectAll:
		return setIntersectAll(left, right)
	}
	return left
}

func setUnionDistinct(left, right [][]any) [][]any {
	seen := make(map[string]bool, len(left)+len(right))
	var result [][]any
	for _, row := range left {
		k := rowKey(row)
		if !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	for _, row := range right {
		k := rowKey(row)
		if !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

func setExceptDistinct(left, right [][]any) [][]any {
	rightSet := make(map[string]bool, len(right))
	for _, row := range right {
		rightSet[rowKey(row)] = true
	}
	seen := make(map[string]bool)
	var result [][]any
	for _, row := range left {
		k := rowKey(row)
		if !rightSet[k] && !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

func setExceptAll(left, right [][]any) [][]any {
	rightCount := make(map[string]int, len(right))
	for _, row := range right {
		rightCount[rowKey(row)]++
	}
	var result [][]any
	for _, row := range left {
		k := rowKey(row)
		if rightCount[k] > 0 {
			rightCount[k]--
		} else {
			result = append(result, row)
		}
	}
	return result
}

func setIntersectDistinct(left, right [][]any) [][]any {
	rightSet := make(map[string]bool, len(right))
	for _, row := range right {
		rightSet[rowKey(row)] = true
	}
	seen := make(map[string]bool)
	var result [][]any
	for _, row := range left {
		k := rowKey(row)
		if rightSet[k] && !seen[k] {
			seen[k] = true
			result = append(result, row)
		}
	}
	return result
}

func setIntersectAll(left, right [][]any) [][]any {
	rightCount := make(map[string]int, len(right))
	for _, row := range right {
		rightCount[rowKey(row)]++
	}
	var result [][]any
	for _, row := range left {
		k := rowKey(row)
		if rightCount[k] > 0 {
			rightCount[k]--
			result = append(result, row)
		}
	}
	return result
}

func sortResultRows(rows [][]any, columns []string, orderBy []OrderByClause) [][]any {
	sort.Slice(rows, func(i, j int) bool {
		for _, ob := range orderBy {
			var colIdx int
			if ob.Pos > 0 && ob.Pos <= len(columns) {
				colIdx = ob.Pos - 1
			} else if ob.Expr != nil {
				continue
			} else {
				for k, c := range columns {
					if c == ob.Column {
						colIdx = k
						break
					}
				}
			}
			if colIdx >= len(rows[i]) || colIdx >= len(rows[j]) {
				continue
			}
			cmp := compareValues(rows[i][colIdx], rows[j][colIdx])
			if ob.Desc {
				cmp = -cmp
			}
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})
	return rows
}

// execViewSelect handles SELECT from a view by executing the view's stored query
// and then applying the outer query's WHERE, projection, ORDER BY, LIMIT on top.
func (e *Executor) execViewSelect(t *txn.Txn, outer *SelectStmt, ref *SimpleTableRef, viewSQL string) (any, error) {
	// Parse the view's SELECT statement
	p := NewParser()
	viewStmt, err := p.Parse(viewSQL)
	if err != nil {
		return nil, fmt.Errorf("view %s: parse error in stored query: %w", ref.Table, err)
	}
	viewSel, ok := viewStmt.(*SelectStmt)
	if !ok {
		return nil, fmt.Errorf("view %s: stored query is not a SELECT", ref.Table)
	}

	// Execute the view's query
	viewResult, err := e.execSelect(t, viewSel)
	if err != nil {
		return nil, err
	}
	selRes, ok := viewResult.(*SelectResult)
	if !ok {
		return nil, fmt.Errorf("view %s: unexpected result type", ref.Table)
	}

	// Build a pseudo tabledef from the view result columns
	alias := ref.Table
	if ref.Alias != "" {
		alias = ref.Alias
	}
	colNames := selRes.Columns
	td := &catalog.TableDef{
		Database: e.dbName,
		Name:     alias,
	}
	for _, colName := range colNames {
		td.Columns = append(td.Columns, storage.ColumnDef{Name: colName, Type: storage.ColTypeInt})
	}

	// Apply outer query's WHERE and projection on the view result rows
	var filteredRows [][]any
	for _, row := range selRes.Rows {
		if outer.Where != nil {
			if !toBool(e.evalExpr(td, outer.Where, row)) {
				continue
			}
		}
		filteredRows = append(filteredRows, row)
	}

	// Apply projection
	var resultRows [][]any
	for _, row := range filteredRows {
		if outer.SelectAll {
			resultRows = append(resultRows, row)
		} else {
			var projected []any
			for _, f := range outer.Fields {
				if f.Expr != nil {
					val := e.evalExpr(td, f.Expr, row)
					projected = append(projected, val)
				} else if f.Column != "" {
					idx := td.ColumnIndex(f.Column)
					if idx >= 0 && idx < len(row) {
						projected = append(projected, row[idx])
					} else {
						projected = append(projected, nil)
					}
				}
			}
			resultRows = append(resultRows, projected)
		}
	}

	// Apply ORDER BY
	if len(outer.OrderBy) > 0 {
		e.sortRowsWithFields(resultRows, colNames, outer.OrderBy, td)
	}

	// Apply LIMIT
	if outer.Limit != nil && *outer.Limit < len(resultRows) {
		resultRows = resultRows[:*outer.Limit]
	}

	// Build result columns
	var resultCols []string
	if outer.SelectAll {
		resultCols = colNames
	} else {
		for _, f := range outer.Fields {
			if f.Alias != "" {
				resultCols = append(resultCols, f.Alias)
			} else if f.Column != "" {
				resultCols = append(resultCols, f.Column)
			} else {
				resultCols = append(resultCols, "?column?")
			}
		}
	}

	return &SelectResult{Rows: resultRows, Columns: resultCols}, nil
}

func (e *Executor) execSelectSimple(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef) (any, error) {
	totalStart := time.Now()
	defer func() {
		metrics.SelectSimpleDuration.Observe(time.Since(totalStart).Seconds())
	}()

	// Check if this is a view reference
	if viewSQL, err := e.cat.GetView(e.dbName, ref.Table); err == nil {
		return e.execViewSelect(t, s, ref, viewSQL)
	}

	tableName := ref.Table
	if ref.Alias != "" {
		tableName = ref.Alias
	}

	// Stage 1: GetTable + column resolution.
	t0 := time.Now()
	td, err := e.cat.GetTable(e.dbName, ref.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()

	// GROUP BY handling — route to dedicated function.
	if len(s.GroupBy) > 0 {
		return e.execSelectGroupBy(t, s, ref, td)
	}

	// Check if this is a pure aggregate query (any field contains aggregate funcs).
	if len(s.Fields) > 0 && !s.SelectAll {
		hasAgg := false
		for _, f := range s.Fields {
			if f.Expr != nil && exprContainsAgg(f.Expr) {
				hasAgg = true
				break
			}
		}
		if hasAgg {
			return e.execSelectAggregateExpr(t, s, ref, td)
		}
	}

	// Build unified projection descriptors from Fields.
	type outputField struct {
		colIdx  int    // column index in td, -1 for expression
		colName string // output column name
		expr    Expr   // non-nil for expression fields
		isExpr  bool
	}
	var outFields []outputField
	var colIndices []int // for backward compat with opt paths
	var colNames []string

	if s.SelectAll {
		for i, col := range td.Columns {
			if col.Hidden {
				continue
			}
			colNames = append(colNames, col.Name)
			colIndices = append(colIndices, i)
			outFields = append(outFields, outputField{colIdx: i, colName: col.Name})
		}
	} else if len(s.Fields) > 0 {
		for _, f := range s.Fields {
			if f.Column != "" {
				idx := td.ColumnIndex(f.Column)
				if idx < 0 {
					return nil, fmt.Errorf("unknown column %q", f.Column)
				}
				name := f.Column
				if f.Alias != "" {
					name = f.Alias
				}
				colIndices = append(colIndices, idx)
				colNames = append(colNames, name)
				outFields = append(outFields, outputField{colIdx: idx, colName: name})
			} else if f.Expr != nil {
				name := f.Alias
				if name == "" {
					name = exprToString(f.Expr)
				}
				colIndices = append(colIndices, -1)
				colNames = append(colNames, name)
				outFields = append(outFields, outputField{colIdx: -1, colName: name, expr: f.Expr, isExpr: true})
			}
		}
	} else {
		// Fallback to old Columns path.
		colIndices = make([]int, len(s.Columns))
		colNames = make([]string, len(s.Columns))
		for i, name := range s.Columns {
			colNames[i] = name
			idx := td.ColumnIndex(name)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column %q", name)
			}
			colIndices[i] = idx
			outFields = append(outFields, outputField{colIdx: idx, colName: name})
		}
	}
	metrics.SelectResolveDuration.Observe(time.Since(t0).Seconds())

	// Stage 2: Optimization path selection.
	t1 := time.Now()
	var rows [][]any

	// For opt paths that only return column values, we may need to re-project.
	hasExprFields := false
	for _, of := range outFields {
		if of.isExpr {
			hasExprFields = true
			break
		}
	}

	// Use opt paths only when there are no expression fields (opt paths return by colIdx).
	if !hasExprFields {
		paths := e.estimateAccessPaths(td, s)
		best := selectBestPath(paths)
		switch best.Type {
		case "pk_point":
			// CBO chose PK point lookup.
			if s.Where != nil {
				if inRows, ok := e.tryINOnPK(t, td, treeKey, s.Where, colIndices); ok {
					rows = inRows
				} else if ptRows, ok := e.tryPointLookupOnPK(t, td, treeKey, s.Where, colIndices); ok {
					rows = ptRows
				}
			}
		case "pk_range":
			// CBO chose PK range scan. Let Stage 3 handle it via extractPKRange.
			// Don't try any fast paths — go straight to the range scan loop.
		case "index_scan", "index_covering":
			// CBO chose secondary index scan. Only try index path.
			if s.Where != nil && len(td.Indexes) > 0 {
				if idxRows, ok := e.tryIndexScan(t, td, s, colIndices, colNames); ok {
					rows = idxRows
				}
			}
		default:
			// CBO chose full_scan. Skip all optimization paths.
			// Fall through to Stage 3 scan loop.
		}
	}
	metrics.SelectOptPathDuration.Observe(time.Since(t1).Seconds())

	// Stage 3: Scan loop (may include t.Scan internally for opt paths).
	// Detect subqueries early so we can use the WithOuter evaluation path.
	hasSubquery := false
	if s.Where != nil {
		hasSubquery = exprContainsSubquery(s.Where)
	}
	if !hasSubquery {
		for _, of := range outFields {
			if of.isExpr && exprContainsSubquery(of.expr) {
				hasSubquery = true
				break
			}
		}
	}

	if rows == nil {
		t2 := time.Now()
		var start, end []byte
		if s.Where != nil {
			start, end = e.extractPKRange(td, s.Where)
		}
		if start == nil {
			start = []byte{0x00}
			end = []byte{0xFF}
			// Diagnose full table scans on large tables.
			if td.Stats != nil && td.Stats.RowCount > 10000 {
				reason := "where_nil"
				if s.Where != nil {
					eqMap := e.collectEqualities(s.Where)
					hasPKEq := false
					for _, pkIdx := range td.PKCols {
						if _, ok := eqMap[td.Columns[pkIdx].Name]; ok {
							hasPKEq = true
							break
						}
					}
					if hasPKEq {
						reason = "coerce_fail"
					} else {
						exprType := fmt.Sprintf("%T", s.Where)
						reason = "no_pk_eq:" + exprType
						// Also record the top-level operator for BinaryExpr.
						if bin, ok := s.Where.(*BinaryExpr); ok {
							reason += "_" + bin.Op
						}
					}
				}
				metrics.FullScanDebug.WithLabelValues(td.Name, reason).Inc()
			}
		}

		// Check if ORDER BY matches PK ascending order and we can stop early.
		limitEarlyStop := -1
		if s.Limit != nil && len(s.OrderBy) > 0 {
			if e.orderByMatchesPKAsc(td, s.OrderBy, s.Where) {
				limitEarlyStop = int(*s.Limit)
			}
		}
		// If no ORDER BY but LIMIT is set, still stop early (rows come in PK order).
		if s.Limit != nil && len(s.OrderBy) == 0 {
			limitEarlyStop = int(*s.Limit)
		}

		alias := ref.Alias
		pkCols := td.PrimaryKeyColumns()
		t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
			vals, _ := storage.DecodeRow(rowData, td.Columns)
			if hasSubquery {
				if s.Where != nil && !e.evalWhereWithOuter(td, vals, s.Where, td, vals, alias, alias) {
					return true
				}
				row := make([]any, len(outFields))
				for i, of := range outFields {
					if of.isExpr {
						row[i] = e.evalExprWithOuter(td, vals, of.expr, td, vals, alias, alias)
					} else {
						row[i] = vals[of.colIdx]
					}
				}
				rows = append(rows, row)
			} else {
				if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
					return true
				}
				row := make([]any, len(outFields))
				for i, of := range outFields {
					if of.isExpr {
						row[i] = e.evalExpr(td, of.expr, vals)
					} else {
						row[i] = vals[of.colIdx]
					}
				}
				rows = append(rows, row)
			}
			if limitEarlyStop > 0 && len(rows) >= limitEarlyStop {
				return false // stop scanning
			}
			return true
		})
		metrics.SelectScanLoopDuration.Observe(time.Since(t2).Seconds())
	} else {
		// Opt path (IN/idx) already completed scan inside opt path — account as scan loop.
		metrics.SelectScanLoopDuration.Observe(time.Since(t1).Seconds())
	}

	// Stage 4: Sort + limit + result building.
	t3 := time.Now()
	if len(s.OrderBy) > 0 {
		e.sortRowsWithFields(rows, colNames, s.OrderBy, td)
	}
	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}
	// Apply DISTINCT
	if s.Distinct {
		rows = dedupRows(rows)
	}
	metrics.SelectPostProcessDuration.Observe(time.Since(t3).Seconds())

	return &SelectResult{Columns: colNames, Rows: rows, TableAlias: tableName}, nil
}

type streamOutputField struct {
	colIdx int
	expr   Expr
	isExpr bool
}

type streamJoinOutputField struct {
	colIdx int
	expr   Expr
	isExpr bool
	name   string
}

func (e *Executor) canStreamSimpleSelect(s *SelectStmt, ref *SimpleTableRef) bool {
	if ref == nil || s.TableRef == nil {
		return false
	}
	if _, err := e.cat.GetView(e.dbName, ref.Table); err == nil {
		return false
	}
	if s.Distinct || len(s.GroupBy) > 0 || len(s.OrderBy) > 0 {
		return false
	}
	if s.Where != nil && exprContainsSubquery(s.Where) {
		return false
	}
	for _, f := range s.Fields {
		if f.Expr != nil && (exprContainsAgg(f.Expr) || exprContainsSubquery(f.Expr)) {
			return false
		}
	}
	return true
}

func (e *Executor) canStreamJoinSelect(s *SelectStmt, ref *JoinTableRef) bool {
	if ref == nil || s.TableRef == nil {
		return false
	}
	if !e.canStreamJoinTables(ref) {
		return false
	}
	if s.Distinct || len(s.GroupBy) > 0 || len(s.OrderBy) > 0 {
		return false
	}
	if s.Where != nil && exprContainsSubquery(s.Where) {
		return false
	}
	if ref.On != nil && exprContainsSubquery(ref.On) {
		return false
	}
	for _, f := range s.Fields {
		if f.Expr != nil && (exprContainsAgg(f.Expr) || exprContainsSubquery(f.Expr)) {
			return false
		}
	}
	for _, expr := range s.SelectExprs {
		if exprContainsAgg(expr) || exprContainsSubquery(expr) {
			return false
		}
	}
	if isAllCrossJoin(ref) {
		return true
	}
	return isTwoTableJoin(ref)
}

func (e *Executor) canStreamJoinTables(ref TableRef) bool {
	switch r := ref.(type) {
	case *SimpleTableRef:
		if _, err := e.cat.GetView(e.dbName, r.Table); err == nil {
			return false
		}
		_, err := e.cat.GetTable(e.dbName, r.Table)
		return err == nil
	case *JoinTableRef:
		return e.canStreamJoinTables(r.Left) && e.canStreamJoinTables(r.Right)
	default:
		return false
	}
}

func isTwoTableJoin(ref *JoinTableRef) bool {
	if ref == nil {
		return false
	}
	_, leftOK := ref.Left.(*SimpleTableRef)
	_, rightOK := ref.Right.(*SimpleTableRef)
	return leftOK && rightOK
}

func (e *Executor) execSelectSimpleStream(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, cleanup func()) (RowIterator, error) {
	td, err := e.cat.GetTable(e.dbName, ref.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()
	colNames, outFields, err := e.resolveStreamOutputFields(td, s)
	if err != nil {
		return nil, err
	}

	start, end := []byte{0x00}, []byte{0xFF}
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
		if start == nil {
			start = []byte{0x00}
			end = []byte{0xFF}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	rows := make(chan []any, 32)
	errc := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer close(rows)
		defer func() {
			if cleanup != nil {
				cleanup()
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				if ee, ok := r.(evalError); ok {
					errc <- ee.err
				} else {
					errc <- fmt.Errorf("stream execution panic: %v", r)
				}
			}
			close(errc)
		}()

		limit := -1
		if s.Limit != nil {
			limit = *s.Limit
		}
		emitted := 0
		pkCols := td.PrimaryKeyColumns()
		t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
			if limit >= 0 && emitted >= limit {
				return false
			}
			vals, _ := storage.DecodeRow(rowData, td.Columns)
			if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
				return true
			}
			row := make([]any, len(outFields))
			for i, of := range outFields {
				if of.isExpr {
					row[i] = e.evalExpr(td, of.expr, vals)
				} else {
					row[i] = vals[of.colIdx]
				}
			}
			select {
			case rows <- row:
				emitted++
				return true
			case <-ctx.Done():
				return false
			}
		})
		errc <- nil
	}()

	return &streamRowIterator{
		columns: colNames,
		rows:    rows,
		errc:    errc,
		cancel:  cancel,
		done:    done,
	}, nil
}

func (e *Executor) resolveStreamOutputFields(td *catalog.TableDef, s *SelectStmt) ([]string, []streamOutputField, error) {
	var colNames []string
	var outFields []streamOutputField

	if s.SelectAll {
		for i, col := range td.Columns {
			if col.Hidden {
				continue
			}
			colNames = append(colNames, col.Name)
			outFields = append(outFields, streamOutputField{colIdx: i})
		}
		return colNames, outFields, nil
	}

	if len(s.Fields) > 0 {
		for _, f := range s.Fields {
			if f.Column != "" {
				idx := td.ColumnIndex(f.Column)
				if idx < 0 {
					return nil, nil, fmt.Errorf("unknown column %q", f.Column)
				}
				name := f.Column
				if f.Alias != "" {
					name = f.Alias
				}
				colNames = append(colNames, name)
				outFields = append(outFields, streamOutputField{colIdx: idx})
			} else if f.Expr != nil {
				name := f.Alias
				if name == "" {
					name = exprToString(f.Expr)
				}
				colNames = append(colNames, name)
				outFields = append(outFields, streamOutputField{colIdx: -1, expr: f.Expr, isExpr: true})
			}
		}
		return colNames, outFields, nil
	}

	for _, name := range s.Columns {
		idx := td.ColumnIndex(name)
		if idx < 0 {
			return nil, nil, fmt.Errorf("unknown column %q", name)
		}
		colNames = append(colNames, name)
		outFields = append(outFields, streamOutputField{colIdx: idx})
	}
	return colNames, outFields, nil
}

func (e *Executor) execSelectJoinStream(t *txn.Txn, s *SelectStmt, ref *JoinTableRef, cleanup func()) (RowIterator, error) {
	tables, err := e.flattenJoinTree(ref)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables in join")
	}
	colNames, outFields, err := e.resolveStreamJoinOutputFields(tables, s)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	rows := make(chan []any, 32)
	errc := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		defer close(rows)
		defer func() {
			if cleanup != nil {
				cleanup()
			}
		}()
		defer func() {
			if r := recover(); r != nil {
				if ee, ok := r.(evalError); ok {
					errc <- ee.err
				} else {
					errc <- fmt.Errorf("stream join execution panic: %v", r)
				}
			}
			close(errc)
		}()

		emitLimit := -1
		if s.Limit != nil {
			emitLimit = *s.Limit
		}
		emitted := 0
		emit := func(joined []any) bool {
			if emitLimit >= 0 && emitted >= emitLimit {
				return false
			}
			row := projectStreamJoinRow(e, tables, joined, outFields)
			select {
			case rows <- row:
				emitted++
				return true
			case <-ctx.Done():
				return false
			}
		}

		if isAllCrossJoin(ref) {
			e.streamCrossJoinRows(ctx, t, tables, tables, s.Where, nil, emit)
		} else {
			e.streamTwoTableJoinRows(ctx, t, s, ref, tables, emit)
		}
		errc <- nil
	}()

	return &streamRowIterator{
		columns: colNames,
		rows:    rows,
		errc:    errc,
		cancel:  cancel,
		done:    done,
	}, nil
}

func (e *Executor) streamCrossJoinRows(ctx context.Context, t *txn.Txn, allTables, remaining []flatTableEntry, where Expr, prefix []any, emit func([]any) bool) bool {
	if len(remaining) == 0 {
		if where != nil && !e.evalWhereMultiJoin(where, allTables, prefix) {
			return true
		}
		return emit(prefix)
	}

	entry := remaining[0]
	return e.scanStreamTable(ctx, t, entry, nil, func(row []any) bool {
		joined := make([]any, 0, len(prefix)+len(row))
		joined = append(joined, prefix...)
		joined = append(joined, row...)
		return e.streamCrossJoinRows(ctx, t, allTables, remaining[1:], where, joined, emit)
	})
}

func (e *Executor) streamTwoTableJoinRows(ctx context.Context, t *txn.Txn, s *SelectStmt, ref *JoinTableRef, tables []flatTableEntry, emit func([]any) bool) bool {
	left := tables[0]
	right := tables[1]
	leftNullRow := make([]any, left.colCount)
	rightNullRow := make([]any, right.colCount)

	if ref.Type == JoinTypeRight {
		return e.scanStreamTable(ctx, t, right, nil, func(rightRow []any) bool {
			matched := false
			keepGoing := e.scanStreamTable(ctx, t, left, nil, func(leftRow []any) bool {
				joined := append(append([]any{}, leftRow...), rightRow...)
				if !e.evalTwoTableJoinOn(ref, tables, joined) {
					return true
				}
				matched = true
				if s.Where != nil && !e.evalWhereMultiJoin(s.Where, tables, joined) {
					return true
				}
				return emit(joined)
			})
			if !keepGoing {
				return false
			}
			if !matched {
				joined := append(append([]any{}, leftNullRow...), rightRow...)
				if s.Where == nil || e.evalWhereMultiJoin(s.Where, tables, joined) {
					return emit(joined)
				}
			}
			return true
		})
	}

	isLeftJoin := ref.Type == JoinTypeLeft
	return e.scanStreamTable(ctx, t, left, nil, func(leftRow []any) bool {
		matched := false
		keepGoing := e.scanStreamTable(ctx, t, right, nil, func(rightRow []any) bool {
			joined := append(append([]any{}, leftRow...), rightRow...)
			if !e.evalTwoTableJoinOn(ref, tables, joined) {
				return true
			}
			matched = true
			if s.Where != nil && !e.evalWhereMultiJoin(s.Where, tables, joined) {
				return true
			}
			return emit(joined)
		})
		if !keepGoing {
			return false
		}
		if !matched && isLeftJoin {
			joined := append(append([]any{}, leftRow...), rightNullRow...)
			if s.Where == nil || e.evalWhereMultiJoin(s.Where, tables, joined) {
				return emit(joined)
			}
		}
		return true
	})
}

func (e *Executor) evalTwoTableJoinOn(ref *JoinTableRef, tables []flatTableEntry, joined []any) bool {
	if ref.On != nil && !e.evalWhereMultiJoin(ref.On, tables, joined) {
		return false
	}
	return true
}

func (e *Executor) scanStreamTable(ctx context.Context, t *txn.Txn, entry flatTableEntry, where Expr, fn func([]any) bool) bool {
	treeKey := entry.td.DataFile()
	pkCols := entry.td.PrimaryKeyColumns()
	keepGoing := true
	t.Scan(treeKey, pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
		select {
		case <-ctx.Done():
			keepGoing = false
			return false
		default:
		}
		vals, _ := storage.DecodeRow(rowData, entry.td.Columns)
		if where != nil && !e.evalWhere(entry.td, where, vals) {
			return true
		}
		keepGoing = fn(vals)
		return keepGoing
	})
	return keepGoing
}

func (e *Executor) resolveStreamJoinOutputFields(tables []flatTableEntry, s *SelectStmt) ([]string, []streamJoinOutputField, error) {
	var fields []streamJoinOutputField

	if s.SelectAll {
		for _, entry := range tables {
			for i, col := range entry.td.Columns {
				if col.Hidden {
					continue
				}
				fields = append(fields, streamJoinOutputField{
					colIdx: entry.colOffset + i,
					name:   entry.alias + "." + col.Name,
				})
			}
		}
		return streamJoinColumnNames(fields), fields, nil
	}

	if len(s.Fields) > 0 {
		for _, f := range s.Fields {
			if f.Column != "" {
				idx, err := resolveColumnOffset(&ColumnRefExpr{Name: f.Column, Table: f.Table}, tables)
				if err != nil {
					return nil, nil, err
				}
				name := f.Column
				if f.Alias != "" {
					name = f.Alias
				}
				fields = append(fields, streamJoinOutputField{colIdx: idx, name: name})
			} else if f.Expr != nil {
				name := f.Alias
				if name == "" {
					name = exprToString(f.Expr)
				}
				fields = append(fields, streamJoinOutputField{colIdx: -1, expr: f.Expr, isExpr: true, name: name})
			}
		}
		return streamJoinColumnNames(fields), fields, nil
	}

	for _, expr := range s.SelectExprs {
		if col, ok := expr.(*ColumnRefExpr); ok {
			idx, err := resolveColumnOffset(col, tables)
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, streamJoinOutputField{colIdx: idx, name: col.Name})
		} else {
			name := exprToString(expr)
			fields = append(fields, streamJoinOutputField{colIdx: -1, expr: expr, isExpr: true, name: name})
		}
	}
	return streamJoinColumnNames(fields), fields, nil
}

func streamJoinColumnNames(fields []streamJoinOutputField) []string {
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.name
	}
	return names
}

func projectStreamJoinRow(e *Executor, tables []flatTableEntry, joined []any, fields []streamJoinOutputField) []any {
	row := make([]any, len(fields))
	for i, f := range fields {
		if f.isExpr {
			row[i] = e.evalExprMultiJoin(f.expr, tables, joined)
		} else if f.colIdx >= 0 && f.colIdx < len(joined) {
			row[i] = joined[f.colIdx]
		}
	}
	return row
}

// tryINOnPK checks if the WHERE clause is a simple col IN (v1, v2, ...) on a
// single-column primary key. If so, it performs point lookups for each value
// and returns (rows, true). Otherwise returns (nil, false).
func (e *Executor) tryINOnPK(t *txn.Txn, td *catalog.TableDef, treeKey string, where Expr, colIndices []int) ([][]any, bool) {
	// Search for IN-on-PK anywhere in the AND tree.
	inExpr, ok := e.findINOnPK(where, td)
	if !ok {
		return nil, false
	}

	pkColIdx := td.PKCols[0]
	pkCol := td.Columns[pkColIdx]

	// Extract literal values from the IN list.
	var vals []any
	for _, v := range inExpr.Values {
		lit := e.extractLiteral(v)
		if lit == nil {
			return nil, false
		}
		coerced, err := storage.CoerceValue(pkCol, lit)
		if err != nil {
			return nil, false
		}
		vals = append(vals, coerced)
	}

	// Point lookup each value.
	var rows [][]any
	pkCols := []storage.ColumnDef{pkCol}
	for _, val := range vals {
		pk := storage.EncodePrimaryKey(pkCols, val)
		rowData, err := t.Get(treeKey, td.Columns, pk)
		if err != nil || rowData == nil {
			continue
		}
		decoded, _ := storage.DecodeRow(rowData, td.Columns)
		row := make([]any, len(colIndices))
		for i, ci := range colIndices {
			row[i] = decoded[ci]
		}
		rows = append(rows, row)
	}
	return rows, true
}

// tryPointLookupOnPK performs a single PK point lookup for equality queries.
// Extracts PK column values from AND-connected equalities and does t.Get().
func (e *Executor) tryPointLookupOnPK(t *txn.Txn, td *catalog.TableDef, treeKey string, where Expr, colIndices []int) ([][]any, bool) {
	eqMap := e.collectEqualities(where)

	// Check that all PK columns have equalities.
	pkVals := make([]any, len(td.PKCols))
	pkCols := make([]storage.ColumnDef, len(td.PKCols))
	for i, colIdx := range td.PKCols {
		col := td.Columns[colIdx]
		pkCols[i] = col
		val, ok := eqMap[col.Name]
		if !ok {
			return nil, false
		}
		coerced, err := storage.CoerceValue(col, val)
		if err != nil {
			return nil, false
		}
		pkVals[i] = coerced
	}

	// Point lookup.
	pk := storage.EncodePrimaryKey(pkCols, pkVals...)
	rowData, err := t.Get(treeKey, td.Columns, pk)
	if err != nil || rowData == nil {
		return nil, true // query handled, but no row found
	}
	decoded, _ := storage.DecodeRow(rowData, td.Columns)
	row := make([]any, len(colIndices))
	for i, ci := range colIndices {
		row[i] = decoded[ci]
	}
	return [][]any{row}, true
}

// tryIndexScan checks if a secondary index can be used for the query.
// Returns (rows, true) if an index was used, (nil, false) otherwise.
func (e *Executor) tryIndexScan(t *txn.Txn, td *catalog.TableDef, s *SelectStmt, colIndices []int, colNames []string) ([][]any, bool) {
	if len(td.Indexes) == 0 {
		metrics.IndexScanAttempts.WithLabelValues("no_idx").Inc()
		return nil, false
	}

	eqMap := e.collectEqualities(s.Where)
	if len(eqMap) == 0 {
		metrics.IndexScanAttempts.WithLabelValues("no_eq").Inc()
		return nil, false
	}

	// If all PK columns have equalities, prefer PK point/range lookup
	// over secondary index scan — it's always more selective.
	pkEqCount := 0
	for _, pkIdx := range td.PKCols {
		if _, ok := eqMap[td.Columns[pkIdx].Name]; ok {
			pkEqCount++
		}
	}
	if pkEqCount == len(td.PKCols) {
		metrics.IndexScanAttempts.WithLabelValues("pk_preferred").Inc()
		return nil, false
	}

	// Find best matching index.
	var bestIdx *catalog.IndexDef
	var bestEqCols int
	for i := range td.Indexes {
		idx := &td.Indexes[i]
		eqCols := 0
		for _, colName := range idx.Columns {
			if _, ok := eqMap[colName]; ok {
				eqCols++
			} else {
				break
			}
		}
		if eqCols > bestEqCols {
			bestEqCols = eqCols
			bestIdx = idx
		}
	}
	if bestIdx == nil || bestEqCols == 0 {
		metrics.IndexScanAttempts.WithLabelValues("no_match").Inc()
		return nil, false
	}

	// Build index scan range.
	idxColDefs := idxColumnDefs(td, bestIdx)
	pkCols := td.PrimaryKeyColumns()
	var prefix []byte
	for i := 0; i < bestEqCols; i++ {
		colName := bestIdx.Columns[i]
		colIdx := td.ColumnIndex(colName)
		col := td.Columns[colIdx]
		val := eqMap[colName]
		coerced, err := storage.CoerceValue(col, val)
		if err != nil {
			metrics.IndexScanAttempts.WithLabelValues("coerce_fail").Inc()
			return nil, false
		}
		prefix = append(prefix, storage.EncodeColumnValue(col, coerced)...)
	}
	start := append([]byte(nil), prefix...)
	end := append(append([]byte(nil), prefix...), 0xFF)

	// Check if covering index: index columns + PK columns contain all select columns.
	coveredSet := make(map[int]bool)
	for _, colName := range bestIdx.Columns {
		coveredSet[td.ColumnIndex(colName)] = true
	}
	for _, pkIdx := range td.PKCols {
		coveredSet[pkIdx] = true
	}
	isCovering := true
	for _, ci := range colIndices {
		if !coveredSet[ci] {
			isCovering = false
			break
		}
	}

	// Scan index tree.
	idxTreeKey := td.IndexFile(bestIdx)
	var rows [][]any
	t.Scan(idxTreeKey, idxColDefs, start, end, func(idxKey, _ []byte) bool {
		pkBytes := storage.DecodeIndexKeyPK(idxKey, idxColDefs)

		if isCovering {
			// Decode values directly from index key.
			vals := make([]any, len(td.Columns))
			offset := 0
			for _, colName := range bestIdx.Columns {
				ci := td.ColumnIndex(colName)
				col := td.Columns[ci]
				val, nextOff := storage.DecodeColumnValue(idxKey, offset, col)
				vals[ci] = val
				offset = nextOff
			}
			// Decode PK columns.
			for i, pkCol := range pkCols {
				val, nextOff := storage.DecodeColumnValue(pkBytes, 0, pkCol)
				vals[td.PKCols[i]] = val
				pkBytes = pkBytes[nextOff:]
			}
			if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
				return true
			}
			row := make([]any, len(colIndices))
			for i, ci := range colIndices {
				row[i] = vals[ci]
			}
			rows = append(rows, row)
		} else {
			// Point lookup on main table.
			rowData, err := t.Get(td.DataFile(), td.Columns, pkBytes)
			if err != nil || rowData == nil {
				return true
			}
			vals, _ := storage.DecodeRow(rowData, td.Columns)
			if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
				return true
			}
			row := make([]any, len(colIndices))
			for i, ci := range colIndices {
				row[i] = vals[ci]
			}
			rows = append(rows, row)
		}
		return true
	})

	// ORDER BY optimization: if ORDER BY column is the next index column
	// after the equality prefix, results are already sorted.
	if len(s.OrderBy) == 1 && bestEqCols < len(bestIdx.Columns) {
		if bestIdx.Columns[bestEqCols] == s.OrderBy[0].Column && !s.OrderBy[0].Desc {
			// Already sorted by index order, skip sortRows.
		} else {
			e.sortRows(rows, colNames, s.OrderBy)
		}
	} else if len(s.OrderBy) > 0 {
		e.sortRows(rows, colNames, s.OrderBy)
	}

	metrics.IndexScanAttempts.WithLabelValues("index_used").Inc()
	return rows, true
}

func (e *Executor) execSelectAggregate(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, td *catalog.TableDef) (any, error) {
	treeKey := td.DataFile()

	var start, end []byte
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
	}
	if start == nil {
		start = []byte{0x00}
		end = []byte{0xFF}
	}

	// Fast path: SELECT COUNT(1) / COUNT(*) without WHERE and without args.
	if s.Where == nil && len(s.SelectExprs) == 1 {
		var countFunc bool
		var hasArgs bool
		if f, ok := s.SelectExprs[0].(*FuncCallExpr); ok && strings.ToUpper(f.Name) == "COUNT" {
			countFunc = true
			hasArgs = len(f.Args) > 0
		} else if f, ok := s.SelectExprs[0].(*AggregateFuncExpr); ok && strings.ToUpper(f.Name) == "COUNT" {
			countFunc = true
			hasArgs = len(f.Args) > 0
		}
		if countFunc && !hasArgs {
			count := e.engine.CountAll(treeKey, start, end)
			colName := "count(1)"
			return &SelectResult{Columns: []string{colName}, Rows: [][]any{{count}}}, nil
		}
	}

	// Fast path: SELECT MIN/MAX(pk_col) WHERE pk_prefix = ?
	// When the aggregate is MIN or MAX on the next PK column after the
	// equality prefix, the B+ tree scan is already ordered, so we only
	// need to read the first (MIN) or last (MAX) matching row.
	if len(s.SelectExprs) == 1 && s.Where != nil && start != nil {
		var funcName string
		var funcArg Expr
		if f, ok := s.SelectExprs[0].(*FuncCallExpr); ok && len(f.Args) == 1 {
			funcName = strings.ToUpper(f.Name)
			funcArg = f.Args[0]
		} else if f, ok := s.SelectExprs[0].(*AggregateFuncExpr); ok && len(f.Args) == 1 {
			funcName = strings.ToUpper(f.Name)
			funcArg = f.Args[0]
		}
		if (funcName == "MIN" || funcName == "MAX") && funcArg != nil {
			if col, ok := funcArg.(*ColumnRefExpr); ok {
				eqMap := e.collectEqualities(s.Where)
				prefixLen := 0
				for _, colIdx := range td.PKCols {
					if _, ok := eqMap[td.Columns[colIdx].Name]; ok {
						prefixLen++
					} else {
						break
					}
				}
				if prefixLen < len(td.PKCols) {
					nextPKColIdx := td.PKCols[prefixLen]
					if td.Columns[nextPKColIdx].Name == col.Name {
						colName := strings.ToLower(funcName)
						if funcName == "MIN" {
							// First row in the scan range is the minimum.
							var minVal any
							e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
								vals, _ := storage.DecodeRow(rowData, td.Columns)
								if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
									return true
								}
								minVal = vals[nextPKColIdx]
								return false // stop after first matching row
							})
							return &SelectResult{Columns: []string{colName}, Rows: [][]any{{minVal}}}, nil
						}
						// MAX: scan to the end and keep the last matching value.
						var maxVal any
						e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
							vals, _ := storage.DecodeRow(rowData, td.Columns)
							if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
								return true
							}
							v := vals[nextPKColIdx]
							if maxVal == nil || compareValues(v, maxVal) > 0 {
								maxVal = v
							}
							return true
						})
						return &SelectResult{Columns: []string{colName}, Rows: [][]any{{maxVal}}}, nil
					}
				}
			}
		}
	}

	type aggState struct {
		count       int64 // COUNT(*) — total rows
		sum         float64
		sumHasValue bool  // true if any non-NULL value was added to SUM
		avgCount    int64 // non-null values for AVG
		minVal      any
		maxVal      any
		hasData     bool
	}

	agg := &aggState{}

	e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}
		agg.hasData = true
		for _, expr := range s.SelectExprs {
			switch f := expr.(type) {
			case *FuncCallExpr, *AggregateFuncExpr:
				var name string
				var args []Expr
				if fc, ok := f.(*FuncCallExpr); ok {
					name = fc.Name
					args = fc.Args
				} else if af, ok := f.(*AggregateFuncExpr); ok {
					name = af.Name
					args = af.Args
				}
				switch strings.ToUpper(name) {
				case "COUNT":
					if len(args) == 0 {
						agg.count++
					} else {
						v := e.evalExpr(td, args[0], vals)
						if v != nil {
							agg.count++
						}
					}
				case "SUM":
					for _, arg := range args {
						v := e.evalExpr(td, arg, vals)
						if v != nil {
							agg.sumHasValue = true
							switch n := v.(type) {
							case int64:
								agg.sum += float64(n)
							case int32:
								agg.sum += float64(n)
							case float64:
								agg.sum += n
							}
						}
					}
				case "AVG":
					for _, arg := range args {
						v := e.evalExpr(td, arg, vals)
						if v != nil {
							agg.avgCount++
							switch n := v.(type) {
							case int64:
								agg.sum += float64(n)
							case int32:
								agg.sum += float64(n)
							case float64:
								agg.sum += n
							}
						}
					}
				case "MIN":
					for _, arg := range args {
						v := e.evalExpr(td, arg, vals)
						if v != nil {
							if agg.minVal == nil || compareValues(v, agg.minVal) < 0 {
								agg.minVal = v
							}
						}
					}
				case "MAX":
					for _, arg := range args {
						v := e.evalExpr(td, arg, vals)
						if v != nil {
							if agg.maxVal == nil || compareValues(v, agg.maxVal) > 0 {
								agg.maxVal = v
							}
						}
					}
				}
			}
		}
		return true
	})

	colNames := make([]string, len(s.SelectExprs))
	row := make([]any, len(s.SelectExprs))
	for i, expr := range s.SelectExprs {
		var name string
		if f, ok := expr.(*FuncCallExpr); ok {
			name = f.Name
		} else if f, ok := expr.(*AggregateFuncExpr); ok {
			name = f.Name
		}
		switch strings.ToUpper(name) {
		case "COUNT":
			row[i] = agg.count
			colNames[i] = "count(1)"
		case "SUM":
			if agg.sumHasValue {
				row[i] = agg.sum
			} else {
				row[i] = nil
			}
		case "AVG":
			if agg.avgCount > 0 {
				row[i] = agg.sum / float64(agg.avgCount)
			} else {
				row[i] = nil
			}
			colNames[i] = "avg"
		case "MIN":
			row[i] = agg.minVal
			colNames[i] = "min"
		case "MAX":
			row[i] = agg.maxVal
			colNames[i] = "max"
		}
	}

	return &SelectResult{Columns: colNames, Rows: [][]any{row}}, nil
}

// execSelectAggregateExpr handles SELECT queries where result expressions
// contain aggregate functions mixed with arithmetic (e.g. COUNT(*) * -31).
// It collects all unique aggregate expressions, computes them over all rows,
// then evaluates each result expression with the aggregate values substituted.
func (e *Executor) execSelectAggregateExpr(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, td *catalog.TableDef) (any, error) {
	treeKey := td.DataFile()

	var start, end []byte
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
	}
	if start == nil {
		start = []byte{0x00}
		end = []byte{0xFF}
	}

	// 1. Collect all unique AggregateFuncExpr nodes from result expressions.
	type aggKey struct {
		name     string
		args     string // serialized args for identity
		distinct bool
	}
	type aggState struct {
		count       int64
		sum         float64
		sumHasValue bool
		avgCount    int64
		minVal      any
		maxVal      any
		distinctSet map[string]bool
	}

	var aggExprs []*AggregateFuncExpr
	seenPtr := make(map[*AggregateFuncExpr]bool)
	var collectAggs func(Expr)
	collectAggs = func(expr Expr) {
		switch ex := expr.(type) {
		case *AggregateFuncExpr:
			if !seenPtr[ex] {
				seenPtr[ex] = true
				aggExprs = append(aggExprs, ex)
			}
		case *BinaryExpr:
			collectAggs(ex.Left)
			collectAggs(ex.Right)
		case *UnaryExpr:
			collectAggs(ex.Operand)
		case *CastExpr:
			collectAggs(ex.Expr)
		case *CaseExpr:
			if ex.Value != nil {
				collectAggs(ex.Value)
			}
			for _, w := range ex.Whens {
				collectAggs(w.Cond)
				collectAggs(w.Result)
			}
			if ex.Else != nil {
				collectAggs(ex.Else)
			}
		case *FuncCallExpr:
			for _, a := range ex.Args {
				collectAggs(a)
			}
		case *IsNullExpr:
			collectAggs(ex.Expr)
		case *BetweenExpr:
			collectAggs(ex.Expr)
			collectAggs(ex.Low)
			collectAggs(ex.High)
		}
	}
	for _, f := range s.Fields {
		if f.Expr != nil {
			collectAggs(f.Expr)
		}
	}

	// 2. Build aggregate state map, keyed by pointer identity of the original expr.
	states := make(map[*AggregateFuncExpr]*aggState)
	for _, ae := range aggExprs {
		st := &aggState{}
		if ae.Distinct {
			st.distinctSet = make(map[string]bool)
		}
		states[ae] = st
	}

	// 3. Scan all rows, updating each aggregate's state.
	e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}
		for ae, st := range states {
			name := strings.ToUpper(ae.Name)
			switch name {
			case "COUNT":
				if len(ae.Args) == 0 {
					st.count++
				} else {
					v := e.evalExpr(td, ae.Args[0], vals)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if !st.distinctSet[k] {
								st.distinctSet[k] = true
								st.count++
							}
						} else {
							st.count++
						}
					}
				}
			case "SUM":
				for _, arg := range ae.Args {
					v := e.evalExpr(td, arg, vals)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						st.sumHasValue = true
						switch n := v.(type) {
						case int32:
							st.sum += float64(n)
						case int64:
							st.sum += float64(n)
						case float64:
							st.sum += n
						}
					}
				}
			case "AVG":
				for _, arg := range ae.Args {
					v := e.evalExpr(td, arg, vals)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						st.avgCount++
						switch n := v.(type) {
						case int32:
							st.sum += float64(n)
						case int64:
							st.sum += float64(n)
						case float64:
							st.sum += n
						}
					}
				}
			case "MIN":
				for _, arg := range ae.Args {
					v := e.evalExpr(td, arg, vals)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						if st.minVal == nil || compareValues(v, st.minVal) < 0 {
							st.minVal = v
						}
					}
				}
			case "MAX":
				for _, arg := range ae.Args {
					v := e.evalExpr(td, arg, vals)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						if st.maxVal == nil || compareValues(v, st.maxVal) > 0 {
							st.maxVal = v
						}
					}
				}
			}
		}
		return true
	})

	// 4. Resolve each aggregate to its final value.
	aggValues := make(map[*AggregateFuncExpr]any)
	for ae, st := range states {
		name := strings.ToUpper(ae.Name)
		switch name {
		case "COUNT":
			aggValues[ae] = st.count
		case "SUM":
			if st.sumHasValue {
				aggValues[ae] = floatToIntIfWhole(st.sum)
			} else {
				aggValues[ae] = nil // SUM of no non-NULL values is NULL
			}
		case "AVG":
			if st.avgCount > 0 {
				aggValues[ae] = st.sum / float64(st.avgCount)
			} else {
				aggValues[ae] = nil
			}
		case "MIN":
			aggValues[ae] = st.minVal
		case "MAX":
			aggValues[ae] = st.maxVal
		}
	}

	// 5. Evaluate each result expression with aggregates substituted.
	colNames := make([]string, len(s.Fields))
	row := make([]any, len(s.Fields))
	for i, f := range s.Fields {
		if f.Alias != "" {
			colNames[i] = f.Alias
		} else if f.Column != "" {
			colNames[i] = f.Column
		} else {
			colNames[i] = exprToString(f.Expr)
		}
		if f.Expr != nil {
			row[i] = e.evalExprWithAgg(td, f.Expr, aggValues)
		} else if f.Column != "" {
			// Column reference in aggregate query — should not happen for valid SQL,
			// but handle gracefully by returning the column value from the last row.
			row[i] = nil
		}
	}

	return &SelectResult{Columns: colNames, Rows: [][]any{row}}, nil
}

// execSelectGroupBy handles SELECT ... GROUP BY ... [HAVING ...] for a single table.
func (e *Executor) execSelectGroupBy(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, td *catalog.TableDef) (any, error) {
	treeKey := td.DataFile()

	var start, end []byte
	if s.Where != nil {
		start, end = e.extractPKRange(td, s.Where)
	}
	if start == nil {
		start = []byte{0x00}
		end = []byte{0xFF}
	}

	alias := ref.Alias

	// 1. Scan all rows (full column values) into memory.
	var allRows [][]any
	e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}
		// Make a copy since vals may be reused.
		row := make([]any, len(vals))
		copy(row, vals)
		allRows = append(allRows, row)
		return true
	})

	// 2. Group rows by GROUP BY key.
	type group struct {
		key  string
		rows [][]any // all rows in this group
	}
	groupMap := make(map[string]*group)
	var groups []*group
	for _, row := range allRows {
		var keyParts []string
		for _, gbExpr := range s.GroupBy {
			v := e.evalExpr(td, gbExpr, row)
			keyParts = append(keyParts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(keyParts, "\x00")
		if g, ok := groupMap[key]; ok {
			g.rows = append(g.rows, row)
		} else {
			g := &group{key: key, rows: [][]any{row}}
			groupMap[key] = g
			groups = append(groups, g)
		}
	}

	// 3. Collect aggregate expressions from SELECT fields and HAVING.
	type aggState struct {
		count       int64
		sum         float64
		sumHasValue bool
		avgCount    int64
		minVal      any
		maxVal      any
		distinctSet map[string]bool
	}

	var aggExprs []*AggregateFuncExpr
	seenPtr := make(map[*AggregateFuncExpr]bool)
	var collectAggs func(Expr)
	collectAggs = func(expr Expr) {
		switch ex := expr.(type) {
		case *AggregateFuncExpr:
			if !seenPtr[ex] {
				seenPtr[ex] = true
				aggExprs = append(aggExprs, ex)
			}
		case *BinaryExpr:
			collectAggs(ex.Left)
			collectAggs(ex.Right)
		case *UnaryExpr:
			collectAggs(ex.Operand)
		case *CastExpr:
			collectAggs(ex.Expr)
		case *CaseExpr:
			if ex.Value != nil {
				collectAggs(ex.Value)
			}
			for _, w := range ex.Whens {
				collectAggs(w.Cond)
				collectAggs(w.Result)
			}
			if ex.Else != nil {
				collectAggs(ex.Else)
			}
		case *FuncCallExpr:
			for _, a := range ex.Args {
				collectAggs(a)
			}
		case *IsNullExpr:
			collectAggs(ex.Expr)
		case *BetweenExpr:
			collectAggs(ex.Expr)
			collectAggs(ex.Low)
			collectAggs(ex.High)
		}
	}
	for _, f := range s.Fields {
		if f.Expr != nil {
			collectAggs(f.Expr)
		}
	}
	if s.Having != nil {
		collectAggs(s.Having)
	}

	// 4. For each group, compute aggregates, evaluate SELECT fields, apply HAVING.
	var resultRows [][]any

	// Build column names and output projection.
	var colNames []string
	type outCol struct {
		colIdx int  // index in td.Columns, -1 for expr
		expr   Expr // for expression fields
		name   string
	}
	var outCols []outCol

	if s.SelectAll {
		for i, col := range td.Columns {
			if col.Hidden {
				continue
			}
			colNames = append(colNames, col.Name)
			outCols = append(outCols, outCol{colIdx: i, name: col.Name})
		}
	} else {
		colNames = make([]string, len(s.Fields))
		for i, f := range s.Fields {
			if f.Alias != "" {
				colNames[i] = f.Alias
			} else if f.Column != "" {
				colNames[i] = f.Column
			} else {
				colNames[i] = exprToString(f.Expr)
			}
			if f.Expr != nil {
				outCols = append(outCols, outCol{colIdx: -1, expr: f.Expr, name: colNames[i]})
			} else if f.Column != "" {
				idx := td.ColumnIndex(f.Column)
				outCols = append(outCols, outCol{colIdx: idx, name: colNames[i]})
			}
		}
	}

	for _, g := range groups {
		// Pick a representative row for column reference evaluation.
		repRow := g.rows[0]

		// Compute aggregates for this group.
		states := make(map[*AggregateFuncExpr]*aggState)
		for _, ae := range aggExprs {
			st := &aggState{}
			if ae.Distinct {
				st.distinctSet = make(map[string]bool)
			}
			states[ae] = st
		}
		for _, row := range g.rows {
			for ae, st := range states {
				name := strings.ToUpper(ae.Name)
				switch name {
				case "COUNT":
					if len(ae.Args) == 0 {
						st.count++
					} else {
						var v any
						if col, ok := ae.Args[0].(*ColumnRefExpr); ok {
							idx := td.ColumnIndex(col.Name)
							if idx >= 0 {
								v = row[idx]
							}
						} else {
							v = e.evalExpr(td, ae.Args[0], row)
						}
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if !st.distinctSet[k] {
									st.distinctSet[k] = true
									st.count++
								}
							} else {
								st.count++
							}
						}
					}
				case "SUM":
					for _, arg := range ae.Args {
						v := e.evalExpr(td, arg, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							st.sumHasValue = true
							switch n := v.(type) {
							case int32:
								st.sum += float64(n)
							case int64:
								st.sum += float64(n)
							case float64:
								st.sum += n
							}
						}
					}
				case "AVG":
					for _, arg := range ae.Args {
						v := e.evalExpr(td, arg, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							st.avgCount++
							switch n := v.(type) {
							case int32:
								st.sum += float64(n)
							case int64:
								st.sum += float64(n)
							case float64:
								st.sum += n
							}
						}
					}
				case "MIN":
					for _, arg := range ae.Args {
						v := e.evalExpr(td, arg, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							if st.minVal == nil || compareValues(v, st.minVal) < 0 {
								st.minVal = v
							}
						}
					}
				case "MAX":
					for _, arg := range ae.Args {
						v := e.evalExpr(td, arg, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							if st.maxVal == nil || compareValues(v, st.maxVal) > 0 {
								st.maxVal = v
							}
						}
					}
				}
			}
		}

		// Resolve aggregates to final values.
		aggValues := make(map[*AggregateFuncExpr]any)
		for ae, st := range states {
			name := strings.ToUpper(ae.Name)
			switch name {
			case "COUNT":
				aggValues[ae] = st.count
			case "SUM":
				if st.sumHasValue {
					aggValues[ae] = floatToIntIfWhole(st.sum)
				} else {
					aggValues[ae] = nil
				}
			case "AVG":
				if st.avgCount > 0 {
					aggValues[ae] = st.sum / float64(st.avgCount)
				} else {
					aggValues[ae] = nil
				}
			case "MIN":
				aggValues[ae] = st.minVal
			case "MAX":
				aggValues[ae] = st.maxVal
			}
		}

		// Evaluate HAVING clause.
		if s.Having != nil {
			hval := e.evalExprWithAggAndRow(td, s.Having, aggValues, repRow, alias)
			if b, ok := hval.(bool); !ok || !b {
				continue
			}
		}

		// Evaluate result row.
		row := make([]any, len(outCols))
		for i, oc := range outCols {
			if oc.expr != nil {
				row[i] = e.evalExprWithAggAndRow(td, oc.expr, aggValues, repRow, alias)
			} else if oc.colIdx >= 0 {
				row[i] = repRow[oc.colIdx]
			}
		}
		resultRows = append(resultRows, row)
	}

	// Apply ORDER BY
	if len(s.OrderBy) > 0 {
		e.sortRowsWithFields(resultRows, colNames, s.OrderBy, td)
	}

	// Apply DISTINCT
	if s.Distinct {
		resultRows = dedupRows(resultRows)
	}

	// Apply LIMIT
	if s.Limit != nil && *s.Limit < len(resultRows) {
		resultRows = resultRows[:*s.Limit]
	}

	tableName := ref.Table
	if ref.Alias != "" {
		tableName = ref.Alias
	}
	return &SelectResult{Columns: colNames, Rows: resultRows, TableAlias: tableName}, nil
}

// evalExprWithAggAndRow evaluates an expression with aggregate values substituted
// and column references resolved from a data row.
func (e *Executor) evalExprWithAggAndRow(td *catalog.TableDef, expr Expr, aggValues map[*AggregateFuncExpr]any, row []any, alias string) any {
	switch ex := expr.(type) {
	case *AggregateFuncExpr:
		if v, ok := aggValues[ex]; ok {
			return v
		}
		return nil
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		colName := ex.Name
		if ex.Table != "" && (ex.Table == alias || ex.Table == td.Name) {
			// Qualified column reference.
		}
		idx := td.ColumnIndex(colName)
		if idx >= 0 {
			return row[idx]
		}
		return nil
	case *BinaryExpr:
		left := e.evalExprWithAggAndRow(td, ex.Left, aggValues, row, alias)
		right := e.evalExprWithAggAndRow(td, ex.Right, aggValues, row, alias)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprWithAggAndRow(td, ex.Operand, aggValues, row, alias)
		return e.evalUnaryOp(ex.Op, operand)
	case *CastExpr:
		v := e.evalExprWithAggAndRow(td, ex.Expr, aggValues, row, alias)
		return castValue(v, ex.Type)
	case *NullExpr:
		return nil
	case *FuncCallExpr:
		var args []any
		for _, a := range ex.Args {
			args = append(args, e.evalExprWithAggAndRow(td, a, aggValues, row, alias))
		}
		return e.evalFuncCallValues(strings.ToUpper(ex.Name), args)
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExprWithAggAndRow(td, ex.Value, aggValues, row, alias)
				cmp := e.evalExprWithAggAndRow(td, w.Cond, aggValues, row, alias)
				if val == nil || cmp == nil {
					cond = false
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExprWithAggAndRow(td, w.Cond, aggValues, row, alias)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExprWithAggAndRow(td, w.Result, aggValues, row, alias)
			}
		}
		if ex.Else != nil {
			return e.evalExprWithAggAndRow(td, ex.Else, aggValues, row, alias)
		}
		return nil
	case *IsNullExpr:
		v := e.evalExprWithAggAndRow(td, ex.Expr, aggValues, row, alias)
		if ex.Not {
			return v != nil
		}
		return v == nil
	case *BetweenExpr:
		val := e.evalExprWithAggAndRow(td, ex.Expr, aggValues, row, alias)
		low := e.evalExprWithAggAndRow(td, ex.Low, aggValues, row, alias)
		high := e.evalExprWithAggAndRow(td, ex.High, aggValues, row, alias)
		if val == nil || low == nil || high == nil {
			return nil
		}
		geLow := compareValues(val, low) >= 0
		leHigh := compareValues(val, high) <= 0
		return geLow && leHigh
	case *InExpr:
		val := e.evalExprWithAggAndRow(td, ex.Expr, aggValues, row, alias)
		if val == nil {
			return nil
		}
		hasNull := false
		for _, v := range ex.Values {
			ev := e.evalExprWithAggAndRow(td, v, aggValues, row, alias)
			if ev == nil {
				hasNull = true
				continue
			}
			if compareValues(val, ev) == 0 {
				if ex.Not {
					return false
				}
				return true
			}
		}
		if hasNull {
			return nil
		}
		if ex.Not {
			return true
		}
		return false
	default:
		return nil
	}
}

// results from the provided map.
func (e *Executor) evalExprWithAgg(td *catalog.TableDef, expr Expr, aggValues map[*AggregateFuncExpr]any) any {
	switch ex := expr.(type) {
	case *AggregateFuncExpr:
		if v, ok := aggValues[ex]; ok {
			return v
		}
		return nil
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		return nil // no row context in aggregate
	case *BinaryExpr:
		left := e.evalExprWithAgg(td, ex.Left, aggValues)
		right := e.evalExprWithAgg(td, ex.Right, aggValues)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprWithAgg(td, ex.Operand, aggValues)
		return e.evalUnaryOp(ex.Op, operand)
	case *CastExpr:
		v := e.evalExprWithAgg(td, ex.Expr, aggValues)
		return castValue(v, ex.Type)
	case *NullExpr:
		return nil
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExprWithAgg(td, ex.Value, aggValues)
				cmp := e.evalExprWithAgg(td, w.Cond, aggValues)
				if val == nil || cmp == nil {
					cond = false
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExprWithAgg(td, w.Cond, aggValues)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExprWithAgg(td, w.Result, aggValues)
			}
		}
		if ex.Else != nil {
			return e.evalExprWithAgg(td, ex.Else, aggValues)
		}
		return nil
	case *FuncCallExpr:
		var args []any
		for _, a := range ex.Args {
			args = append(args, e.evalExprWithAgg(td, a, aggValues))
		}
		return e.evalFuncCallValues(strings.ToUpper(ex.Name), args)
	default:
		return nil
	}
}

// evalFuncCallValues evaluates a function call given already-evaluated argument values.
func (e *Executor) evalFuncCallValues(name string, args []any) any {
	switch name {
	case "COALESCE":
		for _, v := range args {
			if v != nil {
				return v
			}
		}
		return nil
	case "IFNULL":
		if len(args) == 2 {
			if args[0] != nil {
				return args[0]
			}
			return args[1]
		}
	case "ABS":
		if len(args) == 1 {
			switch n := args[0].(type) {
			case int32:
				if n < 0 {
					return -n
				}
				return n
			case int64:
				if n < 0 {
					return -n
				}
				return n
			case float64:
				if n < 0 {
					return -n
				}
				return n
			}
		}
	case "NULLIF":
		if len(args) == 2 {
			if args[0] != nil && args[1] != nil && compareValues(args[0], args[1]) == 0 {
				return nil
			}
			return args[0]
		}
	case "UPPER":
		if len(args) == 1 {
			if s, ok := args[0].(string); ok {
				return strings.ToUpper(s)
			}
		}
	case "LOWER":
		if len(args) == 1 {
			if s, ok := args[0].(string); ok {
				return strings.ToLower(s)
			}
		}
	case "LENGTH", "CHAR_LENGTH":
		if len(args) == 1 {
			if s, ok := args[0].(string); ok {
				return int64(len(s))
			}
		}
	case "TYPEOF":
		if len(args) == 1 {
			return fmt.Sprintf("%T", args[0])
		}
	case "ZEROBLOB":
		if len(args) == 1 {
			if n, ok := toInt64(args[0]); ok && n > 0 {
				return strings.Repeat("\x00", int(n))
			}
			return ""
		}
	case "IIF":
		if len(args) == 3 {
			if b, ok := args[0].(bool); ok && b {
				return args[1]
			}
			return args[2]
		}
	}
	return nil
}

func (e *Executor) execSelectJoin(t *txn.Txn, s *SelectStmt, ref *JoinTableRef) (any, error) {
	// Check if this join query contains aggregate functions in its result fields.
	hasAgg := false
	for _, f := range s.Fields {
		if f.Expr != nil && exprContainsAgg(f.Expr) {
			hasAgg = true
			break
		}
	}

	// If GROUP BY is present, we need to compute the join first, then group.
	if len(s.GroupBy) > 0 {
		return e.execSelectJoinGroupBy(t, s, ref)
	}

	if hasAgg {
		// Execute the join with SELECT * to get all rows, then compute aggregates.
		starStmt := *s
		starStmt.Fields = nil
		starStmt.SelectAll = true
		starStmt.SelectExprs = nil
		starStmt.Columns = nil
		starStmt.Distinct = false
		starStmt.OrderBy = nil
		starStmt.Limit = nil
		var joinRes *SelectResult
		if isAllCrossJoin(ref) {
			res, err := e.execMultiTableJoin(t, &starStmt, ref)
			if err != nil {
				return nil, err
			}
			joinRes = res.(*SelectResult)
		} else {
			res, err := e.execSelectJoinLegacy(t, &starStmt, ref)
			if err != nil {
				return nil, err
			}
			joinRes = res.(*SelectResult)
		}
		return e.computeAggregateOverRows(s, joinRes)
	}

	// For cross joins (comma-separated FROM, no ON condition), use the unified
	// flatten+left-deep path which supports N tables, WHERE pushdown, and projection.
	if isAllCrossJoin(ref) {
		return e.execMultiTableJoin(t, s, ref)
	}

	// For explicit JOIN with ON / LEFT / RIGHT, use the original 2-table path.
	return e.execSelectJoinLegacy(t, s, ref)
}

// execSelectJoinGroupBy handles GROUP BY for multi-table queries.
func (e *Executor) execSelectJoinGroupBy(t *txn.Txn, s *SelectStmt, ref *JoinTableRef) (any, error) {
	// 1. Flatten the join tree to get table entries.
	tables, err := e.flattenJoinTree(ref)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("GROUP BY: no tables in join")
	}

	// 2. Load all table definitions.
	for i := range tables {
		td, err := e.cat.GetTable(e.dbName, tables[i].tableName)
		if err != nil {
			return nil, err
		}
		tables[i].td = td
	}
	// Compute column offsets for visible (non-hidden) columns only,
	// since the join result has been projected to exclude hidden columns.
	offset := 0
	for i := range tables {
		tables[i].colOffset = offset
		visibleCount := 0
		for _, col := range tables[i].td.Columns {
			if !col.Hidden {
				visibleCount++
			}
		}
		tables[i].colCount = visibleCount
		offset += visibleCount
	}

	// 3. Compute the join result (SELECT * to get all columns).
	starStmt := *s
	starStmt.Fields = nil
	starStmt.SelectAll = true
	starStmt.SelectExprs = nil
	starStmt.Columns = nil
	starStmt.Distinct = false
	starStmt.GroupBy = nil
	starStmt.Having = nil
	starStmt.OrderBy = nil
	starStmt.Limit = nil

	var joinRes *SelectResult
	if isAllCrossJoin(ref) {
		res, err := e.execMultiTableJoin(t, &starStmt, ref)
		if err != nil {
			return nil, err
		}
		joinRes = res.(*SelectResult)
	} else {
		res, err := e.execSelectJoinLegacy(t, &starStmt, ref)
		if err != nil {
			return nil, err
		}
		joinRes = res.(*SelectResult)
	}

	allRows := joinRes.Rows

	// 4. Group rows by GROUP BY key.
	type group struct {
		key  string
		rows [][]any
	}
	groupMap := make(map[string]*group)
	var groups []*group
	for _, row := range allRows {
		var keyParts []string
		for _, gbExpr := range s.GroupBy {
			v := e.evalExprMultiJoin(gbExpr, tables, row)
			keyParts = append(keyParts, fmt.Sprintf("%v", v))
		}
		key := strings.Join(keyParts, "\x00")
		if g, ok := groupMap[key]; ok {
			g.rows = append(g.rows, row)
		} else {
			g := &group{key: key, rows: [][]any{row}}
			groupMap[key] = g
			groups = append(groups, g)
		}
	}

	// 5. Collect aggregate expressions from SELECT fields and HAVING.
	type aggState struct {
		count       int64
		sum         float64
		sumHasValue bool
		avgCount    int64
		minVal      any
		maxVal      any
		distinctSet map[string]bool
	}

	var aggExprs []*AggregateFuncExpr
	seenPtr := make(map[*AggregateFuncExpr]bool)
	var collectAggs func(Expr)
	collectAggs = func(expr Expr) {
		switch ex := expr.(type) {
		case *AggregateFuncExpr:
			if !seenPtr[ex] {
				seenPtr[ex] = true
				aggExprs = append(aggExprs, ex)
			}
		case *BinaryExpr:
			collectAggs(ex.Left)
			collectAggs(ex.Right)
		case *UnaryExpr:
			collectAggs(ex.Operand)
		case *CastExpr:
			collectAggs(ex.Expr)
		case *CaseExpr:
			if ex.Value != nil {
				collectAggs(ex.Value)
			}
			for _, w := range ex.Whens {
				collectAggs(w.Cond)
				collectAggs(w.Result)
			}
			if ex.Else != nil {
				collectAggs(ex.Else)
			}
		case *FuncCallExpr:
			for _, a := range ex.Args {
				collectAggs(a)
			}
		case *IsNullExpr:
			collectAggs(ex.Expr)
		case *BetweenExpr:
			collectAggs(ex.Expr)
			collectAggs(ex.Low)
			collectAggs(ex.High)
		}
	}
	for _, f := range s.Fields {
		if f.Expr != nil {
			collectAggs(f.Expr)
		}
	}
	if s.Having != nil {
		collectAggs(s.Having)
	}

	// 6. For each group, compute aggregates, evaluate SELECT, apply HAVING.
	var resultRows [][]any
	var colNames []string
	if s.SelectAll {
		// Build column names from all tables.
		for _, tbl := range tables {
			for _, col := range tbl.td.Columns {
				if !col.Hidden {
					colNames = append(colNames, col.Name)
				}
			}
		}
	} else {
		colNames = make([]string, len(s.Fields))
		for i, f := range s.Fields {
			if f.Alias != "" {
				colNames[i] = f.Alias
			} else if f.Column != "" {
				colNames[i] = f.Column
			} else {
				colNames[i] = exprToString(f.Expr)
			}
		}
	}

	for _, g := range groups {
		repRow := g.rows[0]

		// Compute aggregates for this group.
		states := make(map[*AggregateFuncExpr]*aggState)
		for _, ae := range aggExprs {
			st := &aggState{}
			if ae.Distinct {
				st.distinctSet = make(map[string]bool)
			}
			states[ae] = st
		}
		for _, row := range g.rows {
			for ae, st := range states {
				name := strings.ToUpper(ae.Name)
				switch name {
				case "COUNT":
					if len(ae.Args) == 0 {
						st.count++
					} else {
						v := e.evalExprMultiJoin(ae.Args[0], tables, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if !st.distinctSet[k] {
									st.distinctSet[k] = true
									st.count++
								}
							} else {
								st.count++
							}
						}
					}
				case "SUM":
					for _, arg := range ae.Args {
						v := e.evalExprMultiJoin(arg, tables, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							st.sumHasValue = true
							switch n := v.(type) {
							case int32:
								st.sum += float64(n)
							case int64:
								st.sum += float64(n)
							case float64:
								st.sum += n
							}
						}
					}
				case "AVG":
					for _, arg := range ae.Args {
						v := e.evalExprMultiJoin(arg, tables, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							st.avgCount++
							switch n := v.(type) {
							case int32:
								st.sum += float64(n)
							case int64:
								st.sum += float64(n)
							case float64:
								st.sum += n
							}
						}
					}
				case "MIN":
					for _, arg := range ae.Args {
						v := e.evalExprMultiJoin(arg, tables, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							if st.minVal == nil || compareValues(v, st.minVal) < 0 {
								st.minVal = v
							}
						}
					}
				case "MAX":
					for _, arg := range ae.Args {
						v := e.evalExprMultiJoin(arg, tables, row)
						if v != nil {
							if ae.Distinct {
								k := fmt.Sprintf("%v", v)
								if st.distinctSet[k] {
									continue
								}
								st.distinctSet[k] = true
							}
							if st.maxVal == nil || compareValues(v, st.maxVal) > 0 {
								st.maxVal = v
							}
						}
					}
				}
			}
		}

		// Resolve aggregates to final values.
		aggValues := make(map[*AggregateFuncExpr]any)
		for ae, st := range states {
			name := strings.ToUpper(ae.Name)
			switch name {
			case "COUNT":
				aggValues[ae] = st.count
			case "SUM":
				if st.sumHasValue {
					aggValues[ae] = floatToIntIfWhole(st.sum)
				} else {
					aggValues[ae] = nil
				}
			case "AVG":
				if st.avgCount > 0 {
					aggValues[ae] = st.sum / float64(st.avgCount)
				} else {
					aggValues[ae] = nil
				}
			case "MIN":
				aggValues[ae] = st.minVal
			case "MAX":
				aggValues[ae] = st.maxVal
			}
		}

		// Evaluate HAVING clause.
		if s.Having != nil {
			hval := e.evalExprWithAggMultiJoin(s.Having, aggValues, tables, repRow)
			if b, ok := hval.(bool); !ok || !b {
				continue
			}
		}

		// Evaluate result row.
		if s.SelectAll {
			// Output all non-hidden columns.
			var row []any
			for i, tbl := range tables {
				for j, col := range tbl.td.Columns {
					if !col.Hidden {
						row = append(row, repRow[tbl.colOffset+j])
						_ = i
					}
				}
			}
			resultRows = append(resultRows, row)
		} else {
			row := make([]any, len(s.Fields))
			for i, f := range s.Fields {
				if f.Expr != nil {
					row[i] = e.evalExprWithAggMultiJoin(f.Expr, aggValues, tables, repRow)
				} else if f.Column != "" {
					// Resolve column from the joined row, respecting table qualifier.
					for _, tbl := range tables {
						if f.Table != "" && tbl.tableName != f.Table && tbl.alias != f.Table {
							continue
						}
						idx := tbl.td.ColumnIndex(f.Column)
						if idx >= 0 {
							row[i] = repRow[tbl.colOffset+idx]
							break
						}
					}
				}
			}
			resultRows = append(resultRows, row)
		}
	}

	// Apply ORDER BY
	if len(s.OrderBy) > 0 {
		sortResultRows(resultRows, colNames, s.OrderBy)
	}

	// Apply DISTINCT
	if s.Distinct {
		resultRows = dedupRows(resultRows)
	}

	// Apply LIMIT
	if s.Limit != nil && *s.Limit < len(resultRows) {
		resultRows = resultRows[:*s.Limit]
	}

	return &SelectResult{Columns: colNames, Rows: resultRows}, nil
}

// evalExprWithAggMultiJoin evaluates an expression in a multi-table GROUP BY context.
func (e *Executor) evalExprWithAggMultiJoin(expr Expr, aggValues map[*AggregateFuncExpr]any, tables []flatTableEntry, row []any) any {
	switch ex := expr.(type) {
	case *AggregateFuncExpr:
		if v, ok := aggValues[ex]; ok {
			return v
		}
		return nil
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		idx, err := resolveColumnOffset(ex, tables)
		if err != nil || idx < 0 || idx >= len(row) {
			return nil
		}
		return row[idx]
	case *BinaryExpr:
		left := e.evalExprWithAggMultiJoin(ex.Left, aggValues, tables, row)
		right := e.evalExprWithAggMultiJoin(ex.Right, aggValues, tables, row)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprWithAggMultiJoin(ex.Operand, aggValues, tables, row)
		return e.evalUnaryOp(ex.Op, operand)
	case *CastExpr:
		v := e.evalExprWithAggMultiJoin(ex.Expr, aggValues, tables, row)
		return castValue(v, ex.Type)
	case *NullExpr:
		return nil
	case *FuncCallExpr:
		var args []any
		for _, a := range ex.Args {
			args = append(args, e.evalExprWithAggMultiJoin(a, aggValues, tables, row))
		}
		return e.evalFuncCallValues(strings.ToUpper(ex.Name), args)
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExprWithAggMultiJoin(ex.Value, aggValues, tables, row)
				cmp := e.evalExprWithAggMultiJoin(w.Cond, aggValues, tables, row)
				if val == nil || cmp == nil {
					cond = false
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExprWithAggMultiJoin(w.Cond, aggValues, tables, row)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExprWithAggMultiJoin(w.Result, aggValues, tables, row)
			}
		}
		if ex.Else != nil {
			return e.evalExprWithAggMultiJoin(ex.Else, aggValues, tables, row)
		}
		return nil
	case *IsNullExpr:
		v := e.evalExprWithAggMultiJoin(ex.Expr, aggValues, tables, row)
		if ex.Not {
			return v != nil
		}
		return v == nil
	case *BetweenExpr:
		val := e.evalExprWithAggMultiJoin(ex.Expr, aggValues, tables, row)
		low := e.evalExprWithAggMultiJoin(ex.Low, aggValues, tables, row)
		high := e.evalExprWithAggMultiJoin(ex.High, aggValues, tables, row)
		if val == nil || low == nil || high == nil {
			return nil
		}
		return compareValues(val, low) >= 0 && compareValues(val, high) <= 0
	case *InExpr:
		val := e.evalExprWithAggMultiJoin(ex.Expr, aggValues, tables, row)
		if val == nil {
			return nil
		}
		hasNull := false
		for _, v := range ex.Values {
			ev := e.evalExprWithAggMultiJoin(v, aggValues, tables, row)
			if ev == nil {
				hasNull = true
				continue
			}
			if compareValues(val, ev) == 0 {
				if ex.Not {
					return false
				}
				return true
			}
		}
		if hasNull {
			return nil
		}
		if ex.Not {
			return true
		}
		return false
	default:
		return nil
	}
}

// computeAggregateOverRows computes aggregate functions over a pre-computed
// set of rows (e.g. from a cross join). The joinResult provides the rows
// and column names; the SelectStmt provides the aggregate expressions.
func (e *Executor) computeAggregateOverRows(s *SelectStmt, joinRes *SelectResult) (*SelectResult, error) {
	// 1. Collect all unique AggregateFuncExpr nodes from result expressions.
	type aggKey struct {
		name     string
		args     string
		distinct bool
	}
	type aggState struct {
		count       int64
		sum         float64
		sumHasValue bool
		avgCount    int64
		minVal      any
		maxVal      any
		distinctSet map[string]bool
	}

	var aggExprs []*AggregateFuncExpr
	seenPtr := make(map[*AggregateFuncExpr]bool)
	var collectAggs func(Expr)
	collectAggs = func(expr Expr) {
		switch ex := expr.(type) {
		case *AggregateFuncExpr:
			if !seenPtr[ex] {
				seenPtr[ex] = true
				aggExprs = append(aggExprs, ex)
			}
		case *BinaryExpr:
			collectAggs(ex.Left)
			collectAggs(ex.Right)
		case *UnaryExpr:
			collectAggs(ex.Operand)
		case *CastExpr:
			collectAggs(ex.Expr)
		case *CaseExpr:
			if ex.Value != nil {
				collectAggs(ex.Value)
			}
			for _, w := range ex.Whens {
				collectAggs(w.Cond)
				collectAggs(w.Result)
			}
			if ex.Else != nil {
				collectAggs(ex.Else)
			}
		case *FuncCallExpr:
			for _, a := range ex.Args {
				collectAggs(a)
			}
		}
	}
	for _, f := range s.Fields {
		if f.Expr != nil {
			collectAggs(f.Expr)
		}
	}

	// 2. Build aggregate state map.
	states := make(map[*AggregateFuncExpr]*aggState)
	for _, ae := range aggExprs {
		st := &aggState{}
		if ae.Distinct {
			st.distinctSet = make(map[string]bool)
		}
		states[ae] = st
	}

	// Build column name to index map for evaluating column refs.
	colIndexMap := make(map[string]int)
	for i, name := range joinRes.Columns {
		colIndexMap[name] = i
	}

	// evalExprOverRow evaluates a non-aggregate expression over a single row.
	var evalExprOverRow func(Expr, []any) any
	evalExprOverRow = func(expr Expr, row []any) any {
		switch ex := expr.(type) {
		case *LiteralExpr:
			return ex.Value
		case *ColumnRefExpr:
			name := ex.Name
			if idx, ok := colIndexMap[name]; ok {
				return row[idx]
			}
			return nil
		case *BinaryExpr:
			left := evalExprOverRow(ex.Left, row)
			right := evalExprOverRow(ex.Right, row)
			return e.evalBinaryOp(ex.Op, left, right)
		case *UnaryExpr:
			operand := evalExprOverRow(ex.Operand, row)
			return e.evalUnaryOp(ex.Op, operand)
		case *CastExpr:
			v := evalExprOverRow(ex.Expr, row)
			return castValue(v, ex.Type)
		case *NullExpr:
			return nil
		default:
			return nil
		}
	}

	// 3. Scan all rows, updating each aggregate's state.
	for _, row := range joinRes.Rows {
		for ae, st := range states {
			name := strings.ToUpper(ae.Name)
			switch name {
			case "COUNT":
				if len(ae.Args) == 0 {
					st.count++
				} else {
					v := evalExprOverRow(ae.Args[0], row)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if !st.distinctSet[k] {
								st.distinctSet[k] = true
								st.count++
							}
						} else {
							st.count++
						}
					}
				}
			case "SUM":
				for _, arg := range ae.Args {
					v := evalExprOverRow(arg, row)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						st.sumHasValue = true
						switch n := v.(type) {
						case int32:
							st.sum += float64(n)
						case int64:
							st.sum += float64(n)
						case float64:
							st.sum += n
						}
					}
				}
			case "AVG":
				for _, arg := range ae.Args {
					v := evalExprOverRow(arg, row)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						st.avgCount++
						switch n := v.(type) {
						case int32:
							st.sum += float64(n)
						case int64:
							st.sum += float64(n)
						case float64:
							st.sum += n
						}
					}
				}
			case "MIN":
				for _, arg := range ae.Args {
					v := evalExprOverRow(arg, row)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						if st.minVal == nil || compareValues(v, st.minVal) < 0 {
							st.minVal = v
						}
					}
				}
			case "MAX":
				for _, arg := range ae.Args {
					v := evalExprOverRow(arg, row)
					if v != nil {
						if ae.Distinct {
							k := fmt.Sprintf("%v", v)
							if st.distinctSet[k] {
								continue
							}
							st.distinctSet[k] = true
						}
						if st.maxVal == nil || compareValues(v, st.maxVal) > 0 {
							st.maxVal = v
						}
					}
				}
			}
		}
	}

	// 4. Resolve each aggregate to its final value.
	aggValues := make(map[*AggregateFuncExpr]any)
	for ae, st := range states {
		switch strings.ToUpper(ae.Name) {
		case "COUNT":
			aggValues[ae] = st.count
		case "SUM":
			if st.sumHasValue {
				aggValues[ae] = floatToIntIfWhole(st.sum)
			} else {
				aggValues[ae] = nil
			}
		case "AVG":
			if st.avgCount > 0 {
				aggValues[ae] = st.sum / float64(st.avgCount)
			} else {
				aggValues[ae] = nil
			}
		case "MIN":
			aggValues[ae] = st.minVal
		case "MAX":
			aggValues[ae] = st.maxVal
		}
	}

	// 5. Evaluate result expressions with aggregates substituted.
	colNames := make([]string, len(s.Fields))
	resultRow := make([]any, len(s.Fields))
	for i, f := range s.Fields {
		if f.Alias != "" {
			colNames[i] = f.Alias
		} else if f.Column != "" {
			colNames[i] = f.Column
		} else {
			colNames[i] = exprToString(f.Expr)
		}
		if f.Expr != nil {
			resultRow[i] = e.evalExprWithAgg(nil, f.Expr, aggValues)
		}
	}

	return &SelectResult{Columns: colNames, Rows: [][]any{resultRow}}, nil
}

// isAllCrossJoin returns true if the join tree consists entirely of cross joins
// (no ON conditions, no LEFT/RIGHT join types) — i.e. comma-separated FROM.
func isAllCrossJoin(ref TableRef) bool {
	switch r := ref.(type) {
	case *SimpleTableRef:
		return true
	case *JoinTableRef:
		if r.Type != JoinTypeCross {
			return false
		}
		if r.On != nil {
			return false
		}
		return isAllCrossJoin(r.Left) && isAllCrossJoin(r.Right)
	}
	return false
}

func (e *Executor) execSelectJoinLegacy(t *txn.Txn, s *SelectStmt, ref *JoinTableRef) (any, error) {
	leftTd, err := e.getTableDef(ref.Left)
	if err != nil {
		return nil, err
	}
	rightTd, err := e.getTableDef(ref.Right)
	if err != nil {
		return nil, err
	}

	plan := e.planJoin(ref, s)

	// Use hash join when equi keys are available.
	if plan.method == "hash_join" {
		return e.execHashJoin(t, s, ref, leftTd, rightTd, plan)
	}

	// Nested loop fallback with WHERE pushdown.
	leftRows, err := e.collectRowsWithWhere(t, ref.Left, leftTd, plan.leftWhere)
	if err != nil {
		return nil, err
	}
	rightRows, err := e.collectRowsWithWhere(t, ref.Right, rightTd, plan.rightWhere)
	if err != nil {
		return nil, err
	}

	var rows [][]any
	leftAlias := e.getTableAlias(ref.Left)
	rightAlias := e.getTableAlias(ref.Right)

	isLeftJoin := ref.Type == JoinTypeLeft
	isRightJoin := ref.Type == JoinTypeRight
	leftNullRow := make([]any, len(leftTd.Columns))

	effectiveWhere := plan.remainWhere

	if isRightJoin {
		rightMatched := make(map[int]bool)
		for ri, rightRow := range rightRows {
			matched := false
			for li, leftRow := range leftRows {
				if e.evalJoinCondition(leftTd, rightTd, leftAlias, rightAlias, leftRow, rightRow, ref.On) {
					matched = true
					rightMatched[ri] = true
					joined := append(append([]any{}, leftRow...), rightRow...)
					if effectiveWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, effectiveWhere) {
						rows = append(rows, joined)
					}
					_ = li
				}
			}
			if !matched {
				joined := append(append([]any{}, leftNullRow...), rightRow...)
				if effectiveWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, effectiveWhere) {
					rows = append(rows, joined)
				}
			}
		}
		_ = rightMatched
	} else {
		rightNullRow := make([]any, len(rightTd.Columns))
		for _, leftRow := range leftRows {
			matched := false
			for _, rightRow := range rightRows {
				if e.evalJoinCondition(leftTd, rightTd, leftAlias, rightAlias, leftRow, rightRow, ref.On) {
					matched = true
					joined := append(append([]any{}, leftRow...), rightRow...)
					if effectiveWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, effectiveWhere) {
						rows = append(rows, joined)
					}
				}
			}
			if !matched && isLeftJoin {
				joined := append(append([]any{}, leftRow...), rightNullRow...)
				if effectiveWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, effectiveWhere) {
					rows = append(rows, joined)
				}
			}
		}
	}

	entries, err := e.flattenJoinTree(ref)
	if err != nil {
		return nil, err
	}
	projectedRows, colNames := e.projectMultiJoinResult(s, entries, rows)
	return &SelectResult{Columns: colNames, Rows: projectedRows, TableAlias: leftAlias + " join " + rightAlias}, nil
}

func (e *Executor) collectRows(t *txn.Txn, ref TableRef) ([][]any, error) {
	switch r := ref.(type) {
	case *SimpleTableRef:
		td, err := e.cat.GetTable(e.dbName, r.Table)
		if err != nil {
			return nil, err
		}
		treeKey := td.DataFile()
		var rows [][]any
		pkCols := td.PrimaryKeyColumns()
		t.Scan(treeKey, pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
			vals, _ := storage.DecodeRow(rowData, td.Columns)
			rows = append(rows, vals)
			return true
		})
		return rows, nil
	case *JoinTableRef:
		var allRows [][]any
		leftRows, err := e.collectRows(t, r.Left)
		if err != nil {
			return nil, err
		}
		rightRows, err := e.collectRows(t, r.Right)
		if err != nil {
			return nil, err
		}
		leftTd, _ := e.getTableDef(r.Left)
		rightTd, _ := e.getTableDef(r.Right)
		for _, lr := range leftRows {
			for _, rr := range rightRows {
				if e.evalJoinCondition(leftTd, rightTd, e.getTableAlias(r.Left), e.getTableAlias(r.Right), lr, rr, r.On) {
					joined := append(append([]any{}, lr...), rr...)
					allRows = append(allRows, joined)
				}
			}
		}
		return allRows, nil
	}
	return nil, fmt.Errorf("unsupported table ref")
}

func (e *Executor) getTableDef(ref TableRef) (*catalog.TableDef, error) {
	switch r := ref.(type) {
	case *SimpleTableRef:
		return e.cat.GetTable(e.dbName, r.Table)
	case *JoinTableRef:
		return e.getTableDef(r.Left)
	}
	return nil, fmt.Errorf("unsupported table ref")
}

func (e *Executor) getTableAlias(ref TableRef) string {
	switch r := ref.(type) {
	case *SimpleTableRef:
		if r.Alias != "" {
			return r.Alias
		}
		return r.Table
	case *JoinTableRef:
		return e.getTableAlias(r.Left)
	}
	return ""
}

func (e *Executor) evalJoinCondition(leftTd, rightTd *catalog.TableDef, leftAlias, rightAlias string, leftRow, rightRow []any, on Expr) bool {
	if on == nil {
		return true
	}
	joined := append(append([]any{}, leftRow...), rightRow...)
	return e.evalWhereMultiJoin(on, twoTableJoinEntries(leftTd, rightTd, leftAlias, rightAlias), joined)
}

func (e *Executor) evalJoinWhere(leftTd, rightTd *catalog.TableDef, leftAlias, rightAlias string, joinedRow []any, where Expr) bool {
	return e.evalWhereMultiJoin(where, twoTableJoinEntries(leftTd, rightTd, leftAlias, rightAlias), joinedRow)
}

func twoTableJoinEntries(leftTd, rightTd *catalog.TableDef, leftAlias, rightAlias string) []flatTableEntry {
	if leftAlias == "" {
		leftAlias = leftTd.Name
	}
	if rightAlias == "" {
		rightAlias = rightTd.Name
	}
	return []flatTableEntry{
		{tableName: leftTd.Name, alias: leftAlias, td: leftTd, colOffset: 0},
		{tableName: rightTd.Name, alias: rightAlias, td: rightTd, colOffset: len(leftTd.Columns)},
	}
}

func (e *Executor) execUpdate(t *txn.Txn, s *UpdateStmt) (any, error) {
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
		newVals := make([]any, len(vals))
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

		// Update secondary indexes if any indexed columns changed.
		if len(td.Indexes) > 0 {
			for j := range td.Indexes {
				idx := &td.Indexes[j]
				// Check if any index column changed.
				changed := false
				for _, colName := range idx.Columns {
					ci := td.ColumnIndex(colName)
					if compareValues(vals[ci], newVals[ci]) != 0 {
						changed = true
						break
					}
				}
				if !changed {
					continue
				}
				idxTreeKey := td.IndexFile(idx)
				idxColDefs := idxColumnDefs(td, idx)

				// Delete old index entry using raw pk from scan.
				oldIdxVals := make([]any, len(idx.Columns))
				for k, colName := range idx.Columns {
					oldIdxVals[k] = vals[td.ColumnIndex(colName)]
				}
				oldKey := storage.EncodeIndexKeyWithRawPK(idxColDefs, oldIdxVals, pk)
				t.Delete(idxTreeKey, pkCols, oldKey)

				// Insert new index entry using raw pk from scan.
				newIdxVals := make([]any, len(idx.Columns))
				for k, colName := range idx.Columns {
					newIdxVals[k] = newVals[td.ColumnIndex(colName)]
				}
				newKey := storage.EncodeIndexKeyWithRawPK(idxColDefs, newIdxVals, pk)
				t.Insert(idxTreeKey, newKey, nil)
			}
		}

		if err := t.Update(treeKey, pkCols, pk, newRow); err != nil {
			return false
		}
		affected++
		return true
	})

	return &OKResult{AffectedRows: affected}, nil
}

func (e *Executor) execDelete(t *txn.Txn, s *DeleteStmt) (any, error) {
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

		// Delete from secondary indexes.
		if len(td.Indexes) > 0 {
			for j := range td.Indexes {
				idx := &td.Indexes[j]
				idxTreeKey := td.IndexFile(idx)
				idxColDefs := idxColumnDefs(td, idx)
				idxVals := make([]any, len(idx.Columns))
				for k, colName := range idx.Columns {
					idxVals[k] = vals[td.ColumnIndex(colName)]
				}
				idxKey := storage.EncodeIndexKeyWithRawPK(idxColDefs, idxVals, pk)
				t.Delete(idxTreeKey, pkCols, idxKey)
			}
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

func (e *Executor) execBegin() (any, error) {
	if e.txn != nil {
		return nil, fmt.Errorf("transaction already active")
	}
	e.txn = e.mgr.Begin()
	return &OKResult{}, nil
}

func (e *Executor) execCommit() (any, error) {
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

func (e *Executor) execRollback() (any, error) {
	if e.txn == nil {
		return &OKResult{}, nil
	}
	e.txn.Rollback()
	e.txn = nil
	return &OKResult{}, nil
}

func (e *Executor) execExplain(s *ExplainStmt) (any, error) {
	result := &SelectResult{
		Columns: []string{"id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "Extra"},
	}

	switch inner := s.Inner.(type) {
	case *SelectStmt:
		e.explainSelect(result, inner)
	case *InsertStmt:
		e.explainInsert(result, inner)
	case *UpdateStmt:
		e.explainUpdate(result, inner)
	case *DeleteStmt:
		e.explainDelete(result, inner)
	default:
		result.Rows = append(result.Rows, []any{1, "SIMPLE", "", "ALL", nil, nil, nil, nil, 0, "unsupported"})
	}

	return result, nil
}

func (e *Executor) explainSelect(r *SelectResult, s *SelectStmt) {
	// Handle join explain separately.
	if jRef, ok := s.TableRef.(*JoinTableRef); ok {
		e.explainJoin(r, s, jRef)
		return
	}

	tableName := e.getTableAlias(s.TableRef)
	if tableName == "" {
		tableName = "unknown"
	}

	td, _ := e.cat.GetTable(e.dbName, tableName)

	// Use CBO to determine access path and row estimates.
	var paths []AccessPath
	if td != nil {
		paths = e.estimateAccessPaths(td, s)
	}
	best := selectBestPath(paths)

	// Build EXPLAIN columns.
	var accessType string
	var possibleKeys any
	var usedKey any
	var extras []string

	// Base extras.
	if s.SelectAll {
		extras = append(extras, "select tables scan")
	} else {
		extras = append(extras, "select columns")
	}

	// Determine access type, keys from CBO result.
	switch best.Type {
	case "pk_point":
		accessType = "eq_ref"
		usedKey = "PRIMARY"
		possibleKeys = "PRIMARY"
	case "pk_range":
		accessType = "range"
		usedKey = "PRIMARY"
		possibleKeys = "PRIMARY"
	case "index_covering":
		accessType = "ref"
		usedKey = best.IndexName
		extras = append(extras, "using index")
		possibleKeys = "PRIMARY"
		if best.IndexName != "" {
			possibleKeys = "PRIMARY," + best.IndexName
		}
	case "index_scan":
		accessType = "ref"
		usedKey = best.IndexName
		possibleKeys = "PRIMARY"
		if best.IndexName != "" {
			possibleKeys = "PRIMARY," + best.IndexName
		}
	default:
		accessType = "ALL"
		usedKey = nil
		possibleKeys = nil
	}

	if s.Where != nil {
		extras = append(extras, "using where")
	}

	if len(s.OrderBy) > 0 {
		// Check if ORDER BY satisfied by chosen index.
		needsFilesort := true
		if td != nil && best.IndexName != "" {
			var idx *catalog.IndexDef
			if best.IndexName == "PRIMARY" {
				// PK order check — if ORDER BY matches PK ASC, no filesort.
				if e.orderByMatchesPKAsc(td, s.OrderBy, s.Where) {
					needsFilesort = false
				}
			} else {
				idx = e.findIndexByName(td, best.IndexName)
				if idx != nil && e.orderBySatisfiedByIndex(td, idx, best.MatchCols, s) {
					needsFilesort = false
				}
			}
		}
		if needsFilesort {
			extras = append(extras, "using filesort")
		}
	}

	if s.Limit != nil {
		extras = append(extras, fmt.Sprintf("limit %d", *s.Limit))
	}

	// Add CBO estimates to Extra.
	// Show all considered paths for transparency.
	var cboParts []string
	cboParts = append(cboParts, fmt.Sprintf("est_rows=%d, cost=%.2f", best.EstRows, best.EstCost))
	if td != nil && td.Stats != nil {
		cboParts = append(cboParts, fmt.Sprintf("table_rows=%d", td.Stats.RowCount))
	}
	// Show candidate paths with their costs.
	if len(paths) > 1 {
		var candidates []string
		for _, p := range paths {
			label := p.Type
			if p.IndexName != "" {
				label = p.IndexName + "(" + p.Type + ")"
			}
			candidates = append(candidates, fmt.Sprintf("%s: rows=%d cost=%.2f", label, p.EstRows, p.EstCost))
		}
		cboParts = append(cboParts, "candidates: "+strings.Join(candidates, ", "))
	}
	extras = append(extras, strings.Join(cboParts, "; "))

	r.Rows = append(r.Rows, []any{
		1,
		"SIMPLE",
		tableName,
		accessType,
		possibleKeys,
		usedKey,
		nil,
		nil,
		best.EstRows,
		strings.Join(extras, "; "),
	})
}

// explainJoin adds EXPLAIN rows for a join query.
func (e *Executor) explainJoin(r *SelectResult, s *SelectStmt, ref *JoinTableRef) {
	plan := e.planJoin(ref, s)
	leftAlias := e.getTableAlias(ref.Left)
	rightAlias := e.getTableAlias(ref.Right)

	if plan.method == "hash_join" {
		// Build side row.
		var buildExtras []string
		buildExtras = append(buildExtras, fmt.Sprintf("hash_join(build); est_rows=%d, cost=%.2f", plan.estRows, plan.estCost))
		if plan.buildTd != nil && plan.buildTd.Stats != nil {
			buildExtras = append(buildExtras, fmt.Sprintf("table_rows=%d", plan.buildTd.Stats.RowCount))
		}
		r.Rows = append(r.Rows, []any{
			1, "JOIN", plan.buildAlias, "hash_build",
			nil, nil, nil, nil,
			plan.estBuildRows,
			strings.Join(buildExtras, "; "),
		})

		// Probe side row.
		var probeExtras []string
		probeExtras = append(probeExtras, fmt.Sprintf("hash_join(probe); est_rows=%d, cost=%.2f", plan.estRows, plan.estCost))
		if plan.probeTd != nil && plan.probeTd.Stats != nil {
			probeExtras = append(probeExtras, fmt.Sprintf("table_rows=%d", plan.probeTd.Stats.RowCount))
		}
		// Show the join key reference.
		var refParts []string
		for _, k := range plan.equiKeys {
			refParts = append(refParts, fmt.Sprintf("%s.%s", plan.buildAlias, k.leftName))
		}
		var refVal any
		if len(refParts) > 0 {
			refVal = strings.Join(refParts, ", ")
		}
		r.Rows = append(r.Rows, []any{
			1, "JOIN", plan.probeAlias, "hash_probe",
			nil, nil, nil, refVal,
			plan.estProbeRows,
			strings.Join(probeExtras, "; "),
		})
	} else {
		// Nested loop — show two rows with the original table order.
		leftTd, _ := e.getTableDef(ref.Left)
		rightTd, _ := e.getTableDef(ref.Right)

		var leftExtras []string
		leftExtras = append(leftExtras, fmt.Sprintf("nested_loop_join; est_rows=%d, cost=%.2f", plan.estRows, plan.estCost))
		if leftTd != nil && leftTd.Stats != nil {
			leftExtras = append(leftExtras, fmt.Sprintf("table_rows=%d", leftTd.Stats.RowCount))
		}
		r.Rows = append(r.Rows, []any{
			1, "JOIN", leftAlias, "ALL",
			nil, nil, nil, nil,
			plan.estBuildRows,
			strings.Join(leftExtras, "; "),
		})

		var rightExtras []string
		rightExtras = append(rightExtras, fmt.Sprintf("nested_loop_join; est_rows=%d, cost=%.2f", plan.estRows, plan.estCost))
		if rightTd != nil && rightTd.Stats != nil {
			rightExtras = append(rightExtras, fmt.Sprintf("table_rows=%d", rightTd.Stats.RowCount))
		}
		r.Rows = append(r.Rows, []any{
			1, "JOIN", rightAlias, "ALL",
			nil, nil, nil, nil,
			plan.estProbeRows,
			strings.Join(rightExtras, "; "),
		})
	}
}

func (e *Executor) explainInsert(r *SelectResult, s *InsertStmt) {
	r.Rows = append(r.Rows, []any{
		1,
		"INSERT",
		s.Table,
		"ALL",
		nil,
		nil,
		nil,
		nil,
		0,
		fmt.Sprintf("into %s", s.Table),
	})
}

func (e *Executor) explainUpdate(r *SelectResult, s *UpdateStmt) {
	accessType, possibleKeys, usedKey, extra := e.explainIndexInfoForDML(s.Table, s.Where)
	r.Rows = append(r.Rows, []any{
		1,
		"UPDATE",
		s.Table,
		accessType,
		possibleKeys,
		usedKey,
		nil,
		nil,
		"estimate",
		extra,
	})
}

func (e *Executor) explainDelete(r *SelectResult, s *DeleteStmt) {
	accessType, possibleKeys, usedKey, extra := e.explainIndexInfoForDML(s.Table, s.Where)
	r.Rows = append(r.Rows, []any{
		1,
		"DELETE",
		s.Table,
		accessType,
		possibleKeys,
		usedKey,
		nil,
		nil,
		"estimate",
		extra,
	})
}

// explainIndexInfoForSelect returns (accessType, possibleKeys, usedKey, extra) for a SELECT.
// possibleKeys is a comma-separated string (e.g. "PRIMARY,idx_c_last") for MySQL compatibility.
func (e *Executor) explainIndexInfoForSelect(tableName string, s *SelectStmt) (string, any, any, string) {
	td, _ := e.cat.GetTable(e.dbName, tableName)

	var extras []string
	if s.SelectAll {
		extras = append(extras, "select tables scan")
	} else {
		extras = append(extras, "select columns")
	}

	if s.Where == nil {
		if len(s.OrderBy) > 0 {
			extras = append(extras, "using filesort")
		}
		if s.Limit != nil {
			extras = append(extras, fmt.Sprintf("limit %d", *s.Limit))
		}
		return "ALL", nil, nil, strings.Join(extras, "; ")
	}

	// Determine which index/PK matches best.
	accessType := "range"
	usedKey := any("PRIMARY")

	eqMap := e.collectEqualities(s.Where)
	// Count PK equalities.
	pkEq := 0
	if td != nil {
		for _, pkIdx := range td.PKCols {
			colName := td.Columns[pkIdx].Name
			if _, ok := eqMap[colName]; ok {
				pkEq++
			}
		}
	}

	// Find best secondary index.
	var bestIdxName string
	var bestIdxEqCols int
	if td != nil && len(td.Indexes) > 0 && len(eqMap) > 0 {
		bestIdxName, bestIdxEqCols = e.findBestIndexName(td, eqMap)
	}

	// Build possible_keys list (comma-separated string).
	var possibleKeysList []string
	possibleKeysList = append(possibleKeysList, "PRIMARY")

	// Choose: if PK has all columns matched (point query), prefer PK.
	// Otherwise, if secondary index has more equalities, use it.
	if td != nil && len(td.PKCols) > 0 && pkEq == len(td.PKCols) {
		// Full PK match — PRIMARY wins (point lookup).
		usedKey = "PRIMARY"
	} else if bestIdxName != "" && bestIdxEqCols > 0 {
		possibleKeysList = append(possibleKeysList, bestIdxName)
		if bestIdxEqCols > pkEq {
			usedKey = bestIdxName
		}
	}

	possibleKeys := strings.Join(possibleKeysList, ",")

	// Build extras based on chosen key.
	if td != nil && usedKey == bestIdxName && bestIdxName != "" {
		idx := e.findIndexByName(td, bestIdxName)
		if idx != nil {
			if e.isCoveringIndex(td, idx, s) {
				extras = append(extras, "using index")
			}
			if e.orderBySatisfiedByIndex(td, idx, bestIdxEqCols, s) {
				// ORDER BY satisfied by index order — no filesort.
			} else if len(s.OrderBy) > 0 {
				extras = append(extras, "using filesort")
			}
		}
	} else {
		if len(s.OrderBy) > 0 {
			extras = append(extras, "using filesort")
		}
	}

	if s.Limit != nil {
		extras = append(extras, fmt.Sprintf("limit %d", *s.Limit))
	}

	return accessType, possibleKeys, usedKey, strings.Join(extras, "; ")
}

// explainIndexInfoForDML returns (accessType, possibleKeys, usedKey, extra) for UPDATE/DELETE.
func (e *Executor) explainIndexInfoForDML(table string, where Expr) (string, any, any, string) {
	if where == nil {
		return "ALL", nil, nil, ""
	}

	td, _ := e.cat.GetTable(e.dbName, table)
	possibleKeys := any("PRIMARY")
	usedKey := any("PRIMARY")
	accessType := "range"

	if td == nil || len(td.Indexes) == 0 {
		return accessType, possibleKeys, usedKey, "using where"
	}

	eqMap := e.collectEqualities(where)
	if len(eqMap) == 0 {
		return accessType, possibleKeys, usedKey, "using where"
	}

	// Count PK equalities.
	pkEq := 0
	for _, pkIdx := range td.PKCols {
		colName := td.Columns[pkIdx].Name
		if _, ok := eqMap[colName]; ok {
			pkEq++
		}
	}

	bestIdxName, bestIdxEqCols := e.findBestIndexName(td, eqMap)
	if bestIdxName != "" {
		possibleKeys = fmt.Sprintf("PRIMARY,%s", bestIdxName)
		// Prefer PK if all PK columns have equalities (point query).
		if pkEq < len(td.PKCols) && bestIdxEqCols > pkEq {
			usedKey = bestIdxName
		}
	}

	return accessType, possibleKeys, usedKey, "using where"
}

// findBestIndexName returns (indexName, eqCols) for the index matching the most
// equality prefix columns from eqMap.
func (e *Executor) findBestIndexName(td *catalog.TableDef, eqMap map[string]any) (string, int) {
	var bestName string
	var bestEqCols int
	for i := range td.Indexes {
		idx := &td.Indexes[i]
		eqCols := 0
		for _, colName := range idx.Columns {
			if _, ok := eqMap[colName]; ok {
				eqCols++
			} else {
				break
			}
		}
		if eqCols > bestEqCols {
			bestEqCols = eqCols
			bestName = idx.Name
		}
	}
	return bestName, bestEqCols
}

func (e *Executor) findIndexByName(td *catalog.TableDef, name string) *catalog.IndexDef {
	for i := range td.Indexes {
		if td.Indexes[i].Name == name {
			return &td.Indexes[i]
		}
	}
	return nil
}

// isCoveringIndex checks if the index covers all columns in the SELECT.
func (e *Executor) isCoveringIndex(td *catalog.TableDef, idx *catalog.IndexDef, s *SelectStmt) bool {
	if s.SelectAll {
		return false
	}
	coveredSet := make(map[int]bool)
	for _, colName := range idx.Columns {
		coveredSet[td.ColumnIndex(colName)] = true
	}
	for _, pkIdx := range td.PKCols {
		coveredSet[pkIdx] = true
	}
	for _, expr := range s.SelectExprs {
		colName, ok := expr.(ColumnRefExpr)
		if !ok {
			return false
		}
		ci := td.ColumnIndex(colName.Name)
		if ci < 0 || !coveredSet[ci] {
			return false
		}
	}
	return true
}

// orderBySatisfiedByIndex checks if ORDER BY is already satisfied by the index.
func (e *Executor) orderBySatisfiedByIndex(td *catalog.TableDef, idx *catalog.IndexDef, eqCols int, s *SelectStmt) bool {
	if len(s.OrderBy) == 0 {
		return true
	}
	// Check if the ORDER BY column is the next index column after the equality prefix.
	obCol := s.OrderBy[0].Column
	for i, colName := range idx.Columns {
		if i == eqCols && colName == obCol {
			return true
		}
	}
	return false
}

// ActiveTxn returns the active transaction (for protocol layer).
func (e *Executor) ActiveTxn() *txn.Txn {
	return e.txn
}

// --- Expression evaluation ---

func (e *Executor) evalWhere(td *catalog.TableDef, where Expr, vals []any) bool {
	result := e.evalExpr(td, where, vals)
	if b, ok := result.(bool); ok {
		return b
	}
	if result == nil {
		return false
	}
	return true
}

func (e *Executor) evalExpr(td *catalog.TableDef, expr Expr, vals []any) any {
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
	case *SubqueryExpr:
		return e.execSubquery(ex.Query)
	case *InExpr:
		return e.evalInExpr(td, ex, vals)
	case *ExistsExpr:
		return e.evalExistsExpr(td, ex, vals)
	case *LikeExpr:
		return e.evalLikeExpr(td, ex, vals)
	case *BetweenExpr:
		val := e.evalExpr(td, ex.Expr, vals)
		low := e.evalExpr(td, ex.Low, vals)
		high := e.evalExpr(td, ex.High, vals)
		// BETWEEN is equivalent to val >= low AND val <= high
		// using SQL three-valued logic.
		var geResult any // val >= low
		if val == nil || low == nil {
			geResult = nil
		} else {
			geResult = compareValues(val, low) >= 0
		}
		var leResult any // val <= high
		if val == nil || high == nil {
			leResult = nil
		} else {
			leResult = compareValues(val, high) <= 0
		}
		// AND: false AND anything = false; NULL AND true/NULL = NULL; true AND true = true
		var result any
		geB, geOk := toBoolNil(geResult)
		leB, leOk := toBoolNil(leResult)
		if !geOk || !leOk {
			// At least one is NULL
			if (geOk && !geB) || (leOk && !leB) {
				result = false
			} else {
				result = nil
			}
		} else {
			result = geB && leB
		}
		if ex.Not {
			if result == nil {
				return nil
			}
			return !result.(bool)
		}
		return result
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExpr(td, ex.Value, vals)
				cmp := e.evalExpr(td, w.Cond, vals)
				if val == nil || cmp == nil {
					cond = false // NULL never matches in SQL
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExpr(td, w.Cond, vals)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExpr(td, w.Result, vals)
			}
		}
		if ex.Else != nil {
			return e.evalExpr(td, ex.Else, vals)
		}
		return nil
	case *FuncCallExpr:
		return e.evalFuncCall(td, ex, vals)
	case *CastExpr:
		return e.evalCast(td, ex, vals)
	default:
		return nil
	}
}

func (e *Executor) execSubquery(query *SelectStmt) any {
	// Handle subqueries with no FROM clause (e.g., SELECT 1).
	if query.TableRef == nil {
		row := make([]any, len(query.Fields))
		for i, f := range query.Fields {
			if f.Expr != nil {
				row[i] = e.evalExprNoTable(f.Expr)
			}
		}
		return &SelectResult{Rows: [][]any{row}, Columns: make([]string, len(query.Fields))}
	}
	switch ref := query.TableRef.(type) {
	case *SimpleTableRef:
		txn := e.mgr.Begin()
		defer txn.Rollback()
		result, err := e.execSelectSimple(txn, query, ref)
		if err != nil {
			return nil
		}
		rs, ok := result.(*SelectResult)
		if !ok || len(rs.Rows) == 0 {
			return nil
		}
		if len(rs.Rows) == 1 && len(rs.Rows[0]) == 1 {
			return rs.Rows[0][0]
		}
		return rs
	case *JoinTableRef:
		txn := e.mgr.Begin()
		defer txn.Rollback()
		result, err := e.execSelectJoin(txn, query, ref)
		if err != nil {
			return nil
		}
		rs, ok := result.(*SelectResult)
		if !ok || len(rs.Rows) == 0 {
			return nil
		}
		return rs
	default:
		return nil
	}
}

// evalExprNoTable evaluates an expression without a table context (e.g., SELECT 1).
func (e *Executor) evalExprNoTable(expr Expr) any {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *BinaryExpr:
		left := e.evalExprNoTable(ex.Left)
		right := e.evalExprNoTable(ex.Right)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprNoTable(ex.Operand)
		return e.evalUnaryOp(ex.Op, operand)
	case *FuncCallExpr:
		return e.evalFuncCallNoTable(ex)
	case *CastExpr:
		v := e.evalExprNoTable(ex.Expr)
		return castValue(v, ex.Type)
	case *AggregateFuncExpr:
		// In no-table context, aggregates operate on a single "virtual row".
		name := strings.ToUpper(ex.Name)
		switch name {
		case "COUNT":
			if len(ex.Args) == 0 {
				return int64(1) // COUNT(*) counts the virtual row
			}
			// COUNT(expr): if expr evaluates to NULL, return 0; otherwise 1.
			v := e.evalExprNoTable(ex.Args[0])
			if v == nil {
				return int64(0)
			}
			return int64(1)
		case "SUM", "AVG", "MIN", "MAX":
			if len(ex.Args) == 1 {
				return e.evalExprNoTable(ex.Args[0])
			}
			return nil
		}
		return nil
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExprNoTable(ex.Value)
				cmp := e.evalExprNoTable(w.Cond)
				if val == nil || cmp == nil {
					cond = false
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExprNoTable(w.Cond)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExprNoTable(w.Result)
			}
		}
		if ex.Else != nil {
			return e.evalExprNoTable(ex.Else)
		}
		return nil
	case *NullExpr:
		return nil
	case *IsNullExpr:
		v := e.evalExprNoTable(ex.Expr)
		if ex.Not {
			return v != nil
		}
		return v == nil
	case *BetweenExpr:
		val := e.evalExprNoTable(ex.Expr)
		low := e.evalExprNoTable(ex.Low)
		high := e.evalExprNoTable(ex.High)
		if val == nil || low == nil || high == nil {
			return nil
		}
		result := compareValues(val, low) >= 0 && compareValues(val, high) <= 0
		if ex.Not {
			return !result
		}
		return result
	case *InExpr:
		val := e.evalExprNoTable(ex.Expr)
		if val == nil {
			return nil
		}
		hasNull := false
		for _, v := range ex.Values {
			ev := e.evalExprNoTable(v)
			if ev == nil {
				hasNull = true
				continue
			}
			if compareValues(val, ev) == 0 {
				if ex.Not {
					return false
				}
				return true
			}
		}
		if hasNull {
			return nil
		}
		if ex.Not {
			return true
		}
		return false
	case *SubqueryExpr:
		result := e.execSubquery(ex.Query)
		if rs, ok := result.(*SelectResult); ok && len(rs.Rows) > 0 && len(rs.Rows[0]) > 0 {
			return rs.Rows[0][0]
		}
		return result
	default:
		return nil
	}
}

func (e *Executor) evalFuncCallNoTable(f *FuncCallExpr) any {
	switch strings.ToUpper(f.Name) {
	case "MIN", "MAX", "COUNT", "SUM", "AVG":
		if len(f.Args) == 1 {
			return e.evalExprNoTable(f.Args[0])
		}
	case "COALESCE":
		for _, arg := range f.Args {
			v := e.evalExprNoTable(arg)
			if v != nil {
				return v
			}
		}
		return nil
	case "IFNULL":
		if len(f.Args) == 2 {
			v := e.evalExprNoTable(f.Args[0])
			if v != nil {
				return v
			}
			return e.evalExprNoTable(f.Args[1])
		}
	case "NULLIF":
		if len(f.Args) == 2 {
			v1 := e.evalExprNoTable(f.Args[0])
			v2 := e.evalExprNoTable(f.Args[1])
			if v1 != nil && v2 != nil && compareValues(v1, v2) == 0 {
				return nil
			}
			return v1
		}
	case "ABS":
		if len(f.Args) == 1 {
			v := e.evalExprNoTable(f.Args[0])
			switch n := v.(type) {
			case int32:
				if n < 0 {
					return -n
				}
				return n
			case int64:
				if n < 0 {
					return -n
				}
				return n
			case float64:
				if n < 0 {
					return -n
				}
				return n
			}
		}
	case "UPPER":
		if len(f.Args) == 1 {
			if s, ok := e.evalExprNoTable(f.Args[0]).(string); ok {
				return strings.ToUpper(s)
			}
		}
	case "LOWER":
		if len(f.Args) == 1 {
			if s, ok := e.evalExprNoTable(f.Args[0]).(string); ok {
				return strings.ToLower(s)
			}
		}
	case "LENGTH", "CHAR_LENGTH":
		if len(f.Args) == 1 {
			if s, ok := e.evalExprNoTable(f.Args[0]).(string); ok {
				return int64(len(s))
			}
		}
	case "TYPEOF":
		if len(f.Args) == 1 {
			return fmt.Sprintf("%T", e.evalExprNoTable(f.Args[0]))
		}
	case "IIF":
		if len(f.Args) == 3 {
			cond := e.evalExprNoTable(f.Args[0])
			if b, ok := cond.(bool); ok && b {
				return e.evalExprNoTable(f.Args[1])
			}
			return e.evalExprNoTable(f.Args[2])
		}
	}
	return nil
}

func (e *Executor) evalInExpr(td *catalog.TableDef, expr *InExpr, vals []any) any {
	// Handle empty IN list: IN () → false, NOT IN () → true.
	if expr.Empty {
		return expr.Not
	}
	val := e.evalExpr(td, expr.Expr, vals)
	hasNull := val == nil
	for _, v := range expr.Values {
		if sq, ok := v.(*SubqueryExpr); ok {
			result := e.execSubquery(sq.Query)
			if rs, ok := result.(*SelectResult); ok {
				if len(rs.Columns) > 1 {
					panic(evalError{fmt.Errorf("sub-select returns %d columns - expected 1", len(rs.Columns))})
				}
				for _, row := range rs.Rows {
					if len(row) > 0 && compareValues(val, row[0]) == 0 {
						return !expr.Not
					}
				}
			}
		} else {
			ev := e.evalExpr(td, v, vals)
			if ev == nil {
				hasNull = true
				continue
			}
			if val != nil && compareValues(val, ev) == 0 {
				return !expr.Not
			}
		}
	}
	// No match found.
	// If val is NULL or any list element is NULL → result is NULL.
	if hasNull {
		return nil
	}
	return expr.Not
}

func (e *Executor) evalExistsExpr(td *catalog.TableDef, expr *ExistsExpr, vals []any) bool {
	rs, err := e.execSubqueryWithOuter(expr.Query, td, vals, td.Name)
	if err != nil || rs == nil {
		return expr.Not
	}
	exists := len(rs.Rows) > 0
	if expr.Not {
		return !exists
	}
	return exists
}

func (e *Executor) execSubqueryWithOuter(query *SelectStmt, outerTd *catalog.TableDef, outerVals []any, outerAlias string) (*SelectResult, error) {
	// Handle subqueries with no FROM clause (e.g., SELECT 1).
	if query.TableRef == nil {
		row := make([]any, len(query.Fields))
		for i, f := range query.Fields {
			if f.Expr != nil {
				row[i] = e.evalExprNoTable(f.Expr)
			}
		}
		return &SelectResult{Rows: [][]any{row}, Columns: make([]string, len(query.Fields))}, nil
	}
	switch ref := query.TableRef.(type) {
	case *SimpleTableRef:
		txn := e.mgr.Begin()
		defer txn.Rollback()
		return e.execSelectSimpleWithOuter(txn, query, ref, outerTd, outerVals, outerAlias)
	case *JoinTableRef:
		txn := e.mgr.Begin()
		defer txn.Rollback()
		result, err := e.execSelectJoin(txn, query, ref)
		if err != nil {
			return nil, err
		}
		return result.(*SelectResult), nil
	default:
		return &SelectResult{}, nil
	}
}

func (e *Executor) execSelectSimpleWithOuter(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, outerTd *catalog.TableDef, outerVals []any, outerAlias string) (*SelectResult, error) {
	td, err := e.cat.GetTable(e.dbName, ref.Table)
	if err != nil {
		return nil, err
	}

	innerAlias := ref.Alias

	// Check if this is a pure aggregate query with no outer references in
	// its WHERE clause — if so, we can safely delegate to the optimized
	// aggregate path.  If the WHERE references outer columns, we must
	// evaluate it with outer context here.
	if len(s.Fields) > 0 && !s.SelectAll {
		allAgg := true
		for _, f := range s.Fields {
			if f.Column != "" {
				allAgg = false
				break
			}
			if f.Expr != nil {
				if _, ok := f.Expr.(*AggregateFuncExpr); !ok {
					allAgg = false
					break
				}
			}
		}
		if allAgg && len(s.SelectExprs) > 0 && len(s.Columns) == 0 {
			if outerTd == nil {
				// Non-correlated aggregate — delegate to optimized path.
				result, err := e.execSelectAggregate(t, s, ref, td)
				if err != nil {
					return nil, err
				}
				return result.(*SelectResult), nil
			}
			// Correlated aggregate: handle with outer context.
			return e.execSelectAggregateWithOuter(t, s, ref, td, outerTd, outerVals, innerAlias, outerAlias)
		}
	}

	treeKey := td.DataFile()

	type outputField struct {
		colIdx  int
		colName string
		expr    Expr
		isExpr  bool
	}
	var outFields []outputField
	var colNames []string

	if s.SelectAll {
		colNames = make([]string, len(td.Columns))
		for i, col := range td.Columns {
			colNames[i] = col.Name
			outFields = append(outFields, outputField{colIdx: i, colName: col.Name})
		}
	} else if len(s.Fields) > 0 {
		for _, f := range s.Fields {
			if f.Column != "" {
				idx := td.ColumnIndex(f.Column)
				if idx < 0 {
					idx = 0
				}
				name := f.Column
				if f.Alias != "" {
					name = f.Alias
				}
				colNames = append(colNames, name)
				outFields = append(outFields, outputField{colIdx: idx, colName: name})
			} else if f.Expr != nil {
				name := f.Alias
				if name == "" {
					name = exprToString(f.Expr)
				}
				colNames = append(colNames, name)
				outFields = append(outFields, outputField{colIdx: -1, colName: name, expr: f.Expr, isExpr: true})
			}
		}
	} else if len(s.Columns) > 0 {
		for _, name := range s.Columns {
			idx := td.ColumnIndex(name)
			if idx < 0 {
				idx = 0
			}
			colNames = append(colNames, name)
			outFields = append(outFields, outputField{colIdx: idx, colName: name})
		}
	} else {
		colNames = []string{"id"}
		outFields = append(outFields, outputField{colIdx: 0, colName: "id"})
	}

	start, end := []byte{0x00}, []byte{0xFF}

	var rows [][]any
	pkCols := td.PrimaryKeyColumns()

	t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhereWithOuter(td, vals, s.Where, outerTd, outerVals, innerAlias, outerAlias) {
			return true
		}
		row := make([]any, len(outFields))
		for i, of := range outFields {
			if of.isExpr {
				row[i] = e.evalExprWithOuter(td, vals, of.expr, outerTd, outerVals, innerAlias, outerAlias)
			} else if of.colIdx < len(vals) {
				row[i] = vals[of.colIdx]
			}
		}
		rows = append(rows, row)
		return true
	})

	if len(s.OrderBy) > 0 {
		e.sortRowsWithFields(rows, colNames, s.OrderBy, td)
	}
	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}

	return &SelectResult{Columns: colNames, Rows: rows}, nil
}

func (e *Executor) execSelectAggregateWithOuter(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, td *catalog.TableDef, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) (*SelectResult, error) {
	treeKey := td.DataFile()

	type aggState struct {
		count       int64 // COUNT(*) — total rows
		sum         float64
		sumHasValue bool  // true if any non-NULL value was added to SUM
		avgCount    int64 // non-null values for AVG
		minVal      any
		maxVal      any
		hasData     bool
	}

	agg := &aggState{}

	pkCols := td.PrimaryKeyColumns()
	t.Scan(treeKey, pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhereWithOuter(td, vals, s.Where, outerTd, outerVals, innerAlias, outerAlias) {
			return true
		}
		agg.hasData = true
		for _, expr := range s.SelectExprs {
			switch f := expr.(type) {
			case *FuncCallExpr, *AggregateFuncExpr:
				var name string
				var args []Expr
				if fc, ok := f.(*FuncCallExpr); ok {
					name = fc.Name
					args = fc.Args
				} else if af, ok := f.(*AggregateFuncExpr); ok {
					name = af.Name
					args = af.Args
				}
				switch strings.ToUpper(name) {
				case "COUNT":
					if len(args) == 0 {
						agg.count++
					} else {
						v := e.evalExprWithOuter(td, vals, args[0], outerTd, outerVals, innerAlias, outerAlias)
						if v != nil {
							agg.count++
						}
					}
				case "SUM":
					for _, arg := range args {
						v := e.evalExprWithOuter(td, vals, arg, outerTd, outerVals, innerAlias, outerAlias)
						if v != nil {
							switch n := v.(type) {
							case int64:
								agg.sum += float64(n)
							case int32:
								agg.sum += float64(n)
							case float64:
								agg.sum += n
							}
						}
					}
				case "AVG":
					for _, arg := range args {
						v := e.evalExprWithOuter(td, vals, arg, outerTd, outerVals, innerAlias, outerAlias)
						if v != nil {
							agg.avgCount++
							switch n := v.(type) {
							case int64:
								agg.sum += float64(n)
							case int32:
								agg.sum += float64(n)
							case float64:
								agg.sum += n
							}
						}
					}
				case "MIN":
					for _, arg := range args {
						v := e.evalExprWithOuter(td, vals, arg, outerTd, outerVals, innerAlias, outerAlias)
						if v != nil {
							if agg.minVal == nil || compareValues(v, agg.minVal) < 0 {
								agg.minVal = v
							}
						}
					}
				case "MAX":
					for _, arg := range args {
						v := e.evalExprWithOuter(td, vals, arg, outerTd, outerVals, innerAlias, outerAlias)
						if v != nil {
							if agg.maxVal == nil || compareValues(v, agg.maxVal) > 0 {
								agg.maxVal = v
							}
						}
					}
				}
			}
		}
		return true
	})

	colNames := make([]string, len(s.SelectExprs))
	row := make([]any, len(s.SelectExprs))
	for i, expr := range s.SelectExprs {
		var name string
		if f, ok := expr.(*FuncCallExpr); ok {
			name = f.Name
		} else if f, ok := expr.(*AggregateFuncExpr); ok {
			name = f.Name
		}
		switch strings.ToUpper(name) {
		case "COUNT":
			row[i] = agg.count
			colNames[i] = "count(1)"
		case "SUM":
			if agg.sumHasValue {
				row[i] = agg.sum
			} else {
				row[i] = nil
			}
		case "AVG":
			if agg.avgCount > 0 {
				row[i] = agg.sum / float64(agg.avgCount)
			} else {
				row[i] = nil
			}
			colNames[i] = "avg"
		case "MIN":
			row[i] = agg.minVal
			colNames[i] = "min"
		case "MAX":
			row[i] = agg.maxVal
			colNames[i] = "max"
		}
	}

	return &SelectResult{Columns: colNames, Rows: [][]any{row}}, nil
}

func (e *Executor) evalWhereWithOuter(td *catalog.TableDef, vals []any, where Expr, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) bool {
	result := e.evalExprWithOuter(td, vals, where, outerTd, outerVals, innerAlias, outerAlias)
	if b, ok := result.(bool); ok {
		return b
	}
	if result == nil {
		return false
	}
	return true
}

func (e *Executor) evalExprWithOuter(td *catalog.TableDef, vals []any, expr Expr, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) any {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		colName := ex.Name
		tableName := ex.Table
		if tableName != "" {
			// Qualified reference: determine if it refers to the inner or outer table.
			innerMatch := false
			if innerAlias != "" {
				innerMatch = (tableName == innerAlias)
			} else {
				innerMatch = (tableName == td.Name)
			}
			if innerMatch {
				idx := td.ColumnIndex(colName)
				if idx >= 0 {
					return vals[idx]
				}
				return nil
			}
			// Check outer table (by real name or by alias).
			if outerTd != nil {
				outerMatch := (tableName == outerTd.Name)
				if outerAlias != "" {
					outerMatch = outerMatch || (tableName == outerAlias)
				}
				if outerMatch {
					idx := outerTd.ColumnIndex(colName)
					if idx >= 0 {
						return outerVals[idx]
					}
				}
			}
			return nil
		}
		// Unqualified: inner first, then outer.
		idx := td.ColumnIndex(colName)
		if idx >= 0 {
			return vals[idx]
		}
		if outerTd != nil {
			idx = outerTd.ColumnIndex(colName)
			if idx >= 0 {
				return outerVals[idx]
			}
		}
		return nil
	case *BinaryExpr:
		left := e.evalExprWithOuter(td, vals, ex.Left, outerTd, outerVals, innerAlias, outerAlias)
		right := e.evalExprWithOuter(td, vals, ex.Right, outerTd, outerVals, innerAlias, outerAlias)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprWithOuter(td, vals, ex.Operand, outerTd, outerVals, innerAlias, outerAlias)
		return e.evalUnaryOp(ex.Op, operand)
	case *NullExpr:
		return nil
	case *IsNullExpr:
		v := e.evalExprWithOuter(td, vals, ex.Expr, outerTd, outerVals, innerAlias, outerAlias)
		if ex.Not {
			return v != nil
		}
		return v == nil
	case *SubqueryExpr:
		result, err := e.execSubqueryWithOuter(ex.Query, outerTd, outerVals, outerAlias)
		if err != nil || result == nil {
			return nil
		}
		if len(result.Rows) == 1 && len(result.Rows[0]) == 1 {
			return result.Rows[0][0]
		}
		return result
	case *InExpr:
		return e.evalInExprWithOuter(td, vals, ex, outerTd, outerVals, innerAlias, outerAlias)
	case *ExistsExpr:
		return e.evalExistsExprWithOuter(td, vals, ex, outerTd, outerVals, innerAlias, outerAlias)
	case *LikeExpr:
		return e.evalLikeExprWithOuter(td, vals, ex, outerTd, outerVals, innerAlias, outerAlias)
	case *BetweenExpr:
		val := e.evalExprWithOuter(td, vals, ex.Expr, outerTd, outerVals, innerAlias, outerAlias)
		low := e.evalExprWithOuter(td, vals, ex.Low, outerTd, outerVals, innerAlias, outerAlias)
		high := e.evalExprWithOuter(td, vals, ex.High, outerTd, outerVals, innerAlias, outerAlias)
		if val == nil || low == nil || high == nil {
			return nil
		}
		cmpLow := compareValues(val, low)
		cmpHigh := compareValues(val, high)
		result := cmpLow >= 0 && cmpHigh <= 0
		if ex.Not {
			result = !result
		}
		return result
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExprWithOuter(td, vals, ex.Value, outerTd, outerVals, innerAlias, outerAlias)
				cmp := e.evalExprWithOuter(td, vals, w.Cond, outerTd, outerVals, innerAlias, outerAlias)
				if val == nil || cmp == nil {
					cond = false // NULL never matches in SQL
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExprWithOuter(td, vals, w.Cond, outerTd, outerVals, innerAlias, outerAlias)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExprWithOuter(td, vals, w.Result, outerTd, outerVals, innerAlias, outerAlias)
			}
		}
		if ex.Else != nil {
			return e.evalExprWithOuter(td, vals, ex.Else, outerTd, outerVals, innerAlias, outerAlias)
		}
		return nil
	case *FuncCallExpr:
		return e.evalFuncCallWithOuter(td, ex, vals, outerTd, outerVals, innerAlias, outerAlias)
	default:
		return nil
	}
}

func (e *Executor) evalInExprWithOuter(td *catalog.TableDef, vals []any, expr *InExpr, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) bool {
	// Handle empty IN list: IN () → false, NOT IN () → true.
	if expr.Empty {
		return expr.Not
	}
	val := e.evalExprWithOuter(td, vals, expr.Expr, outerTd, outerVals, innerAlias, outerAlias)
	hasNull := val == nil
	for _, v := range expr.Values {
		if sq, ok := v.(*SubqueryExpr); ok {
			result, err := e.execSubqueryWithOuter(sq.Query, outerTd, outerVals, outerAlias)
			if err != nil || result == nil {
				return expr.Not
			}
			if len(result.Columns) > 1 {
				panic(evalError{fmt.Errorf("sub-select returns %d columns - expected 1", len(result.Columns))})
			}
			for _, row := range result.Rows {
				if len(row) > 0 && compareValues(val, row[0]) == 0 {
					return !expr.Not
				}
			}
		} else {
			ev := e.evalExprWithOuter(td, vals, v, outerTd, outerVals, innerAlias, outerAlias)
			if ev == nil {
				hasNull = true
				continue
			}
			if val != nil && compareValues(val, ev) == 0 {
				return !expr.Not
			}
		}
	}
	// No match found.
	if hasNull {
		return false
	}
	return expr.Not
}

func (e *Executor) evalExistsExprWithOuter(td *catalog.TableDef, vals []any, expr *ExistsExpr, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) bool {
	rs, err := e.execSubqueryWithOuter(expr.Query, outerTd, outerVals, outerAlias)
	if err != nil {
		return expr.Not
	}
	exists := len(rs.Rows) > 0
	if expr.Not {
		return !exists
	}
	return exists
}

func (e *Executor) evalLikeExprWithOuter(td *catalog.TableDef, vals []any, expr *LikeExpr, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) bool {
	val := e.evalExprWithOuter(td, vals, expr.Expr, outerTd, outerVals, innerAlias, outerAlias)
	pattern := e.evalExprWithOuter(td, vals, expr.Pattern, outerTd, outerVals, innerAlias, outerAlias)
	result := e.evalLike(val, pattern)
	if expr.Not {
		return !result
	}
	return result
}

func (e *Executor) evalBinaryOp(op string, left, right any) any {
	switch op {
	case "=":
		if left == nil || right == nil {
			return nil
		}
		return compareValues(left, right) == 0
	case "!=":
		if left == nil || right == nil {
			return nil
		}
		return compareValues(left, right) != 0
	case "<":
		if left == nil || right == nil {
			return nil
		}
		return compareValues(left, right) < 0
	case "<=":
		if left == nil || right == nil {
			return nil
		}
		return compareValues(left, right) <= 0
	case ">":
		if left == nil || right == nil {
			return nil
		}
		return compareValues(left, right) > 0
	case ">=":
		if left == nil || right == nil {
			return nil
		}
		return compareValues(left, right) >= 0
	case "AND":
		// SQL three-valued logic: NULL AND false = false, NULL AND true = NULL
		lb, lok := toBoolNil(left)
		rb, rok := toBoolNil(right)
		if !lok || !rok {
			// At least one is NULL
			if (lok && !lb) || (rok && !rb) {
				return false // NULL AND false = false
			}
			return nil // NULL AND true/NULL = NULL
		}
		return lb && rb
	case "OR":
		// SQL three-valued logic: NULL OR true = true, NULL OR false = NULL
		lb, lok := toBoolNil(left)
		rb, rok := toBoolNil(right)
		if !lok || !rok {
			// At least one is NULL
			if (lok && lb) || (rok && rb) {
				return true // NULL OR true = true
			}
			return nil // NULL OR false/NULL = NULL
		}
		return lb || rb
	case "IN":
		return e.evalIn(left, right)
	case "LIKE":
		return e.evalLike(left, right)
	case "+":
		return arithOp(left, right, func(a, b int64) int64 { return a + b }, func(a, b float64) float64 { return a + b })
	case "-":
		return arithOp(left, right, func(a, b int64) int64 { return a - b }, func(a, b float64) float64 { return a - b })
	case "*":
		return arithOp(left, right, func(a, b int64) int64 { return a * b }, func(a, b float64) float64 { return a * b })
	case "/":
		return arithOp(left, right, func(a, b int64) int64 {
			if b == 0 {
				return 0
			}
			return a / b
		}, func(a, b float64) float64 {
			if b == 0 {
				return 0
			}
			return a / b
		})
	case "%":
		return arithOp(left, right, func(a, b int64) int64 {
			if b == 0 {
				return 0
			}
			return a % b
		}, func(a, b float64) float64 {
			if b == 0 {
				return 0
			}
			return float64(int64(a) % int64(b))
		})
	default:
		return nil
	}
}

func (e *Executor) evalIn(left, right any) bool {
	if right == nil {
		return false
	}
	if rs, ok := right.(*SelectResult); ok {
		for _, row := range rs.Rows {
			if len(row) > 0 && compareValues(left, row[0]) == 0 {
				return true
			}
		}
		return false
	}
	return compareValues(left, right) == 0
}

func (e *Executor) evalLike(val, pattern any) bool {
	if val == nil || pattern == nil {
		return false
	}
	s, ok := val.(string)
	if !ok {
		s = fmt.Sprintf("%v", val)
	}
	p, ok := pattern.(string)
	if !ok {
		p = fmt.Sprintf("%v", pattern)
	}
	return matchLike(s, p)
}

func (e *Executor) evalLikeExpr(td *catalog.TableDef, expr *LikeExpr, vals []any) bool {
	val := e.evalExpr(td, expr.Expr, vals)
	pattern := e.evalExpr(td, expr.Pattern, vals)
	result := e.evalLike(val, pattern)
	if expr.Not {
		return !result
	}
	return result
}

func matchLike(s, pattern string) bool {
	regex := likeToRegex(pattern)
	matched, _ := regexp.MatchString("^"+regex+"$", s)
	return matched
}

func likeToRegex(pattern string) string {
	var result strings.Builder
	i := 0
	for i < len(pattern) {
		c := pattern[i]
		if c == '\\' && i+1 < len(pattern) {
			result.WriteString(regexp.QuoteMeta(string(pattern[i+1])))
			i += 2
			continue
		}
		switch c {
		case '%':
			result.WriteString(".*")
		case '_':
			result.WriteString(".")
		default:
			result.WriteString(regexp.QuoteMeta(string(c)))
		}
		i++
	}
	return result.String()
}

func (e *Executor) evalUnaryOp(op string, operand any) any {
	switch op {
	case "+":
		return operand
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
		if operand == nil {
			return nil
		}
		return !toBool(operand)
	}
	return nil
}

func (e *Executor) evalFuncCall(td *catalog.TableDef, f *FuncCallExpr, vals []any) any {
	switch strings.ToUpper(f.Name) {
	case "COALESCE":
		for _, arg := range f.Args {
			v := e.evalExpr(td, arg, vals)
			if v != nil {
				return v
			}
		}
		return nil
	case "IFNULL":
		if len(f.Args) == 2 {
			v := e.evalExpr(td, f.Args[0], vals)
			if v != nil {
				return v
			}
			return e.evalExpr(td, f.Args[1], vals)
		}
	case "ABS":
		if len(f.Args) == 1 {
			v := e.evalExpr(td, f.Args[0], vals)
			switch n := v.(type) {
			case int32:
				if n < 0 {
					return -n
				}
				return n
			case int64:
				if n < 0 {
					return -n
				}
				return n
			case float64:
				if n < 0 {
					return -n
				}
				return n
			}
		}
	case "UPPER":
		if len(f.Args) == 1 {
			v := e.evalExpr(td, f.Args[0], vals)
			if s, ok := v.(string); ok {
				return strings.ToUpper(s)
			}
		}
	case "LOWER":
		if len(f.Args) == 1 {
			v := e.evalExpr(td, f.Args[0], vals)
			if s, ok := v.(string); ok {
				return strings.ToLower(s)
			}
		}
	case "LENGTH", "CHAR_LENGTH":
		if len(f.Args) == 1 {
			v := e.evalExpr(td, f.Args[0], vals)
			if s, ok := v.(string); ok {
				return int64(len(s))
			}
		}
	case "TYPEOF":
		if len(f.Args) == 1 {
			v := e.evalExpr(td, f.Args[0], vals)
			return fmt.Sprintf("%T", v)
		}
	case "NULLIF":
		if len(f.Args) == 2 {
			v1 := e.evalExpr(td, f.Args[0], vals)
			v2 := e.evalExpr(td, f.Args[1], vals)
			if compareValues(v1, v2) == 0 {
				return nil
			}
			return v1
		}
	case "IIF":
		if len(f.Args) == 3 {
			cond := e.evalExpr(td, f.Args[0], vals)
			if b, ok := cond.(bool); ok && b {
				return e.evalExpr(td, f.Args[1], vals)
			}
			return e.evalExpr(td, f.Args[2], vals)
		}
	case "ZEROBLOB":
		if len(f.Args) == 1 {
			v := e.evalExpr(td, f.Args[0], vals)
			if n, ok := toInt64(v); ok && n > 0 {
				return strings.Repeat("\x00", int(n))
			}
			return ""
		}
	}
	return nil
}

func (e *Executor) evalCast(td *catalog.TableDef, c *CastExpr, vals []any) any {
	val := e.evalExpr(td, c.Expr, vals)
	return castValue(val, c.Type)
}

func evalCastNoTable(c *CastExpr) any {
	// For no-table context, we can't use evalExpr. Handle simple literals.
	return castValue(evalExprSimpleNoTable(c.Expr), c.Type)
}

// floatToIntIfWhole converts a float64 to int64 if it represents a whole number.
// This ensures that SUM of integer columns produces int64 results, enabling
// integer division semantics in subsequent arithmetic.
func floatToIntIfWhole(f float64) any {
	if f == float64(int64(f)) && !math.IsInf(f, 0) && !math.IsNaN(f) {
		return int64(f)
	}
	return f
}

func castValue(val any, targetType string) any {
	if val == nil {
		return nil
	}
	tpUpper := strings.ToUpper(targetType)
	// Common patterns: "SIGNED", "SIGNED INTEGER", "UNSIGNED", "DOUBLE", "FLOAT", "CHAR", "BINARY"
	if strings.Contains(tpUpper, "SIGNED") || strings.Contains(tpUpper, "INT") {
		switch v := val.(type) {
		case int64:
			return v
		case int32:
			return int64(v)
		case int:
			return int64(v)
		case float64:
			return int64(v)
		case string:
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				return n
			}
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return int64(f)
			}
		case bool:
			if v {
				return int64(1)
			}
			return int64(0)
		}
		return int64(0)
	}
	if strings.Contains(tpUpper, "UNSIGNED") {
		switch v := val.(type) {
		case int64:
			if v >= 0 {
				return v
			}
			return int64(0)
		case float64:
			if v >= 0 {
				return int64(v)
			}
			return int64(0)
		}
		return int64(0)
	}
	if strings.Contains(tpUpper, "DOUBLE") || strings.Contains(tpUpper, "FLOAT") || strings.Contains(tpUpper, "REAL") {
		switch v := val.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case int32:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
		return float64(0)
	}
	// Default: return as-is for CHAR/VARCHAR/BINARY etc.
	return val
}

// evalExprSimpleNoTable evaluates simple expressions without a table context.
// Used by evalCastNoTable and similar functions where full evalExprNoTable isn't available.
func evalExprSimpleNoTable(expr Expr) any {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *UnaryExpr:
		operand := evalExprSimpleNoTable(ex.Operand)
		return evalUnaryOpSimple(ex.Op, operand)
	case *CastExpr:
		return castValue(evalExprSimpleNoTable(ex.Expr), ex.Type)
	case *FuncCallExpr:
		return nil // Complex - would need full evalExprNoTable
	case *BinaryExpr:
		return nil
	default:
		return nil
	}
}

func evalUnaryOpSimple(op string, operand any) any {
	switch op {
	case "-", "UMINUS":
		switch v := operand.(type) {
		case int64:
			return -v
		case float64:
			return -v
		case int:
			return -v
		}
	case "+", "UPLUS":
		return operand
	case "NOT":
		if b, ok := toBoolNil(operand); ok {
			return !b
		}
		return nil
	}
	return nil
}

// Returns nil start/end if it can't optimize.
// Supports equality conditions on consecutive PK columns, plus one
// range condition (>=, >, <=, <, BETWEEN) on the next PK column.
func (e *Executor) evalFuncCallWithOuter(td *catalog.TableDef, f *FuncCallExpr, vals []any, outerTd *catalog.TableDef, outerVals []any, innerAlias, outerAlias string) any {
	switch strings.ToUpper(f.Name) {
	case "COALESCE":
		for _, arg := range f.Args {
			v := e.evalExprWithOuter(td, vals, arg, outerTd, outerVals, innerAlias, outerAlias)
			if v != nil {
				return v
			}
		}
		return nil
	case "IFNULL":
		if len(f.Args) == 2 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			if v != nil {
				return v
			}
			return e.evalExprWithOuter(td, vals, f.Args[1], outerTd, outerVals, innerAlias, outerAlias)
		}
	case "ABS":
		if len(f.Args) == 1 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			switch n := v.(type) {
			case int32:
				if n < 0 {
					return -n
				}
				return n
			case int64:
				if n < 0 {
					return -n
				}
				return n
			case float64:
				if n < 0 {
					return -n
				}
				return n
			}
		}
	case "UPPER":
		if len(f.Args) == 1 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			if s, ok := v.(string); ok {
				return strings.ToUpper(s)
			}
		}
	case "LOWER":
		if len(f.Args) == 1 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			if s, ok := v.(string); ok {
				return strings.ToLower(s)
			}
		}
	case "LENGTH", "CHAR_LENGTH":
		if len(f.Args) == 1 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			if s, ok := v.(string); ok {
				return int64(len(s))
			}
		}
	case "TYPEOF":
		if len(f.Args) == 1 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			return fmt.Sprintf("%T", v)
		}
	case "NULLIF":
		if len(f.Args) == 2 {
			v1 := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			v2 := e.evalExprWithOuter(td, vals, f.Args[1], outerTd, outerVals, innerAlias, outerAlias)
			if compareValues(v1, v2) == 0 {
				return nil
			}
			return v1
		}
	case "IIF":
		if len(f.Args) == 3 {
			cond := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			if b, ok := cond.(bool); ok && b {
				return e.evalExprWithOuter(td, vals, f.Args[1], outerTd, outerVals, innerAlias, outerAlias)
			}
			return e.evalExprWithOuter(td, vals, f.Args[2], outerTd, outerVals, innerAlias, outerAlias)
		}
	case "ZEROBLOB":
		if len(f.Args) == 1 {
			v := e.evalExprWithOuter(td, vals, f.Args[0], outerTd, outerVals, innerAlias, outerAlias)
			if n, ok := toInt64(v); ok && n > 0 {
				return strings.Repeat("\x00", int(n))
			}
			return ""
		}
	}
	return nil
}

// exprContainsAgg returns true if the expression tree contains any AggregateFuncExpr.
func exprContainsAgg(expr Expr) bool {
	switch ex := expr.(type) {
	case *AggregateFuncExpr:
		return true
	case *BinaryExpr:
		return exprContainsAgg(ex.Left) || exprContainsAgg(ex.Right)
	case *UnaryExpr:
		return exprContainsAgg(ex.Operand)
	case *CastExpr:
		return exprContainsAgg(ex.Expr)
	case *IsNullExpr:
		return exprContainsAgg(ex.Expr)
	case *BetweenExpr:
		return exprContainsAgg(ex.Expr) || exprContainsAgg(ex.Low) || exprContainsAgg(ex.High)
	case *InExpr:
		if exprContainsAgg(ex.Expr) {
			return true
		}
		for _, v := range ex.Values {
			if exprContainsAgg(v) {
				return true
			}
		}
		return false
	case *CaseExpr:
		if ex.Value != nil && exprContainsAgg(ex.Value) {
			return true
		}
		for _, w := range ex.Whens {
			if exprContainsAgg(w.Cond) || exprContainsAgg(w.Result) {
				return true
			}
		}
		if ex.Else != nil && exprContainsAgg(ex.Else) {
			return true
		}
		return false
	case *FuncCallExpr:
		for _, a := range ex.Args {
			if exprContainsAgg(a) {
				return true
			}
		}
		return false
	case *SubqueryExpr:
		return false
	}
	return false
}

// exprContainsSubquery returns true if the expression tree contains a
// SubqueryExpr or ExistsExpr (i.e. it needs outer-row context to evaluate).
func exprContainsSubquery(expr Expr) bool {
	switch ex := expr.(type) {
	case *SubqueryExpr:
		return true
	case *ExistsExpr:
		return true
	case *BinaryExpr:
		return exprContainsSubquery(ex.Left) || exprContainsSubquery(ex.Right)
	case *UnaryExpr:
		return exprContainsSubquery(ex.Operand)
	case *IsNullExpr:
		return exprContainsSubquery(ex.Expr)
	case *InExpr:
		if exprContainsSubquery(ex.Expr) {
			return true
		}
		for _, v := range ex.Values {
			if exprContainsSubquery(v) {
				return true
			}
		}
		return false
	case *LikeExpr:
		return exprContainsSubquery(ex.Expr) || exprContainsSubquery(ex.Pattern)
	case *BetweenExpr:
		return exprContainsSubquery(ex.Expr) || exprContainsSubquery(ex.Low) || exprContainsSubquery(ex.High)
	case *CaseExpr:
		if ex.Value != nil && exprContainsSubquery(ex.Value) {
			return true
		}
		for _, w := range ex.Whens {
			if exprContainsSubquery(w.Cond) || exprContainsSubquery(w.Result) {
				return true
			}
		}
		return ex.Else != nil && exprContainsSubquery(ex.Else)
	case *FuncCallExpr:
		for _, arg := range ex.Args {
			if exprContainsSubquery(arg) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

func (e *Executor) extractPKRange(td *catalog.TableDef, where Expr) ([]byte, []byte) {
	// Collect all PK column equalities from the WHERE clause.
	eqMap := e.collectEqualities(where)

	// Build a prefix range from consecutive PK columns.
	var prefix []byte
	rangeStartIdx := 0
	for i, colIdx := range td.PKCols {
		col := td.Columns[colIdx]
		val, ok := eqMap[col.Name]
		if !ok {
			rangeStartIdx = i
			break
		}
		coerced, err := storage.CoerceValue(col, val)
		if err != nil {
			rangeStartIdx = i
			break
		}
		prefix = append(prefix, storage.EncodeColumnValue(col, coerced)...)
		rangeStartIdx = i + 1
	}

	if len(prefix) == 0 {
		return nil, nil
	}

	// Default range: [prefix, prefix + 0xFF]
	start := append([]byte(nil), prefix...)
	end := append(append([]byte(nil), prefix...), 0xFF)

	// Check for range conditions on the next PK column after the equality prefix.
	if rangeStartIdx < len(td.PKCols) {
		col := td.Columns[td.PKCols[rangeStartIdx]]
		lowVal, highVal, hasLow, hasHigh, lowInclusive, highInclusive := e.collectRangeBounds(where, col.Name)
		if hasLow {
			lowEncoded := storage.EncodeColumnValue(col, lowVal)
			if !lowInclusive {
				lowEncoded = encodeNextValue(lowEncoded)
			}
			start = append(append([]byte(nil), prefix...), lowEncoded...)
		}
		if hasHigh {
			highEncoded := storage.EncodeColumnValue(col, highVal)
			if highInclusive {
				// Include all keys with this prefix value: append 0xFF sentinel
				end = append(append(append([]byte(nil), prefix...), highEncoded...), 0xFF)
			} else {
				// Exclusive: the encoded value itself is the boundary
				end = append(append([]byte(nil), prefix...), highEncoded...)
			}
		}
	}

	return start, end
}

// rangeBounds holds collected lower/upper bounds for a single column.
type rangeBounds struct {
	lowVal, highVal   any
	hasLow, hasHigh   bool
	lowIncl, highIncl bool
}

// collectRangeBounds scans AND-connected conditions for range bounds on the given column.
func (e *Executor) collectRangeBounds(expr Expr, colName string) (lowVal, highVal any, hasLow, hasHigh, lowIncl, highIncl bool) {
	bounds := &rangeBounds{}
	e.collectRangeBoundsInto(expr, colName, bounds)
	return bounds.lowVal, bounds.highVal, bounds.hasLow, bounds.hasHigh, bounds.lowIncl, bounds.highIncl
}

func (e *Executor) collectRangeBoundsInto(expr Expr, colName string, b *rangeBounds) {
	// Handle AND: recurse into both sides.
	if bin, ok := expr.(*BinaryExpr); ok && bin.Op == "AND" {
		e.collectRangeBoundsInto(bin.Left, colName, b)
		e.collectRangeBoundsInto(bin.Right, colName, b)
		return
	}

	// Handle BETWEEN col AND low AND high.
	if bt, ok := expr.(*BetweenExpr); ok {
		if col, ok := bt.Expr.(*ColumnRefExpr); ok && col.Name == colName {
			low := e.extractLiteral(bt.Low)
			high := e.extractLiteral(bt.High)
			if low != nil && high != nil {
				b.hasLow, b.lowVal, b.lowIncl = true, low, true
				b.hasHigh, b.highVal, b.highIncl = true, high, true
			}
		}
		return
	}

	// Handle comparison operators: col >= val, col > val, col <= val, col < val.
	bin, ok := expr.(*BinaryExpr)
	if !ok {
		return
	}

	var colSide bool // true if col is on the left
	var col *ColumnRefExpr
	var val any
	if c, ok := bin.Left.(*ColumnRefExpr); ok && c.Name == colName {
		col = c
		val = e.extractLiteral(bin.Right)
		colSide = true
	} else if c, ok := bin.Right.(*ColumnRefExpr); ok && c.Name == colName {
		col = c
		val = e.extractLiteral(bin.Left)
		colSide = false
	}
	if col == nil || val == nil {
		return
	}

	switch bin.Op {
	case ">=":
		if colSide {
			b.hasLow, b.lowVal, b.lowIncl = true, val, true
		} else {
			b.hasHigh, b.highVal, b.highIncl = true, val, true
		}
	case ">":
		if colSide {
			b.hasLow, b.lowVal, b.lowIncl = true, val, false
		} else {
			b.hasHigh, b.highVal, b.highIncl = true, val, false
		}
	case "<=":
		if colSide {
			b.hasHigh, b.highVal, b.highIncl = true, val, true
		} else {
			b.hasLow, b.lowVal, b.lowIncl = true, val, true
		}
	case "<":
		if colSide {
			b.hasHigh, b.highVal, b.highIncl = true, val, false
		} else {
			b.hasLow, b.lowVal, b.lowIncl = true, val, false
		}
	}
}

// encodeNextValue returns the smallest byte sequence that sorts after the given value.
// For order-preserving int encoding: increment the last byte, or append 0x00 if
// that would overflow. For simplicity, we append a 0x00 byte to ensure we sort
// after all keys with exactly this value.
func encodeNextValue(encoded []byte) []byte {
	return append(append([]byte(nil), encoded...), 0x00)
}

// findINOnPK searches the WHERE expression tree (handling AND) for an InExpr
// whose column covers all PK columns. For single-column PKs, this matches
// `col IN (v1, v2, ...)`. Returns the InExpr and true if found.
func (e *Executor) findINOnPK(where Expr, td *catalog.TableDef) (*InExpr, bool) {
	if where == nil {
		return nil, false
	}
	if inExpr, ok := where.(*InExpr); ok {
		if e.inExprMatchesPK(inExpr, td) {
			return inExpr, true
		}
		return nil, false
	}
	bin, ok := where.(*BinaryExpr)
	if !ok || bin.Op != "AND" {
		return nil, false
	}
	if found, ok := e.findINOnPK(bin.Left, td); ok {
		return found, true
	}
	return e.findINOnPK(bin.Right, td)
}

// inExprMatchesPK checks if an InExpr references all PK columns.
// For single-column PK: col IN (...) where col is the PK column.
func (e *Executor) inExprMatchesPK(inExpr *InExpr, td *catalog.TableDef) bool {
	if inExpr.Not || len(inExpr.Values) == 0 {
		return false
	}
	col, ok := inExpr.Expr.(*ColumnRefExpr)
	if !ok {
		return false
	}
	// Single-column PK check
	if len(td.PKCols) != 1 {
		return false
	}
	return td.Columns[td.PKCols[0]].Name == col.Name
}

// collectEqualities extracts col=val pairs from AND-connected equalities.
func (e *Executor) collectEqualities(expr Expr) map[string]any {
	result := make(map[string]any)
	e.collectEqualitiesInto(expr, result)
	return result
}

func (e *Executor) collectEqualitiesInto(expr Expr, m map[string]any) {
	binExpr, ok := expr.(*BinaryExpr)
	if !ok {
		// Handle single-value IN as equality: col IN (val) ≡ col = val
		if inExpr, ok := expr.(*InExpr); ok && !inExpr.Not && len(inExpr.Values) == 1 {
			if col, ok := inExpr.Expr.(*ColumnRefExpr); ok {
				val := e.extractLiteral(inExpr.Values[0])
				if col.Name != "" && val != nil {
					m[col.Name] = val
				}
			}
		}
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
	var val any
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

func (e *Executor) extractLiteral(expr Expr) any {
	if lit, ok := expr.(*LiteralExpr); ok {
		return lit.Value
	}
	return nil
}

// orderByMatchesPKAsc returns true if the ORDER BY columns match the PK columns
// in ascending order, accounting for the equality prefix from the WHERE clause.
// For example, if PK is (a, b, c), WHERE a=? AND b=? and ORDER BY c ASC,
// the scan results are already sorted by c within the (a, b) prefix.
func (e *Executor) orderByMatchesPKAsc(td *catalog.TableDef, orderBy []OrderByClause, where Expr) bool {
	if len(orderBy) == 0 {
		return false
	}

	// Count how many PK prefix columns have equalities in WHERE.
	eqMap := e.collectEqualities(where)
	prefixLen := 0
	for _, colIdx := range td.PKCols {
		if _, ok := eqMap[td.Columns[colIdx].Name]; ok {
			prefixLen++
		} else {
			break
		}
	}

	// ORDER BY must match PK columns starting from prefixLen.
	if prefixLen+len(orderBy) > len(td.PKCols) {
		return false
	}
	for i, ob := range orderBy {
		if ob.Desc {
			return false
		}
		pkColIdx := td.PKCols[prefixLen+i]
		if ob.Column != td.Columns[pkColIdx].Name {
			return false
		}
	}
	return true
}

func (e *Executor) sortRows(rows [][]any, colNames []string, orderBy []OrderByClause) {
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
			if e.lessThan(rows[j], rows[j-1], orderBy, colIdx, nil) {
				rows[j], rows[j-1] = rows[j-1], rows[j]
			} else {
				break
			}
		}
	}
}

// sortRowsWithFields sorts rows supporting expression and positional ORDER BY.
func (e *Executor) sortRowsWithFields(rows [][]any, colNames []string, orderBy []OrderByClause, td *catalog.TableDef) {
	if len(orderBy) == 0 || len(rows) <= 1 {
		return
	}
	colIdx := make(map[string]int)
	for i, name := range colNames {
		colIdx[name] = i
	}

	for i := 1; i < len(rows); i++ {
		for j := i; j > 0; j-- {
			if e.lessThanWithTD(rows[j], rows[j-1], orderBy, colIdx, td) {
				rows[j], rows[j-1] = rows[j-1], rows[j]
			} else {
				break
			}
		}
	}
}

func (e *Executor) lessThan(a, b []any, orderBy []OrderByClause, colIdx map[string]int, td *catalog.TableDef) bool {
	for _, ob := range orderBy {
		var av, bv any
		if ob.Pos > 0 {
			idx := ob.Pos - 1
			if idx < len(a) {
				av = a[idx]
			}
			if idx < len(b) {
				bv = b[idx]
			}
		} else if ob.Expr != nil && td != nil {
			// Expression-based ORDER BY: we can't re-evaluate without original vals.
			// This case is handled by lessThanWithTD.
			continue
		} else {
			idx := colIdx[ob.Column]
			av = a[idx]
			bv = b[idx]
		}
		cmp := compareValues(av, bv)
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

func (e *Executor) lessThanWithTD(a, b []any, orderBy []OrderByClause, colIdx map[string]int, td *catalog.TableDef) bool {
	for _, ob := range orderBy {
		var av, bv any
		if ob.Pos > 0 {
			idx := ob.Pos - 1
			if idx < len(a) {
				av = a[idx]
			}
			if idx < len(b) {
				bv = b[idx]
			}
		} else if ob.Column != "" {
			idx, ok := colIdx[ob.Column]
			if ok && idx < len(a) && idx < len(b) {
				av = a[idx]
				bv = b[idx]
			}
		}
		// Note: expression-based ORDER BY should be pre-computed as an extra column
		// during scan. For now, we handle positional and column-based ORDER BY.
		cmp := compareValues(av, bv)
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
	case "INT", "INTEGER":
		return storage.ColTypeInt
	case "BIGINT":
		return storage.ColTypeBigInt
	case "VARCHAR", "TEXT", "CHAR", "CLOB":
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

func compareValues(a, b any) int {
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
		case float64:
			af := float64(av)
			if af < bv {
				return -1
			}
			if af > bv {
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
			return 0
		case int32:
			bv64 := int64(bv)
			if av < bv64 {
				return -1
			}
			if av > bv64 {
				return 1
			}
			return 0
		case float64:
			af := float64(av)
			if af < bv {
				return -1
			}
			if af > bv {
				return 1
			}
			return 0
		default:
			return 0
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			if av < bv {
				return -1
			}
			if av > bv {
				return 1
			}
			return 0
		case int32:
			bf := float64(bv)
			if av < bf {
				return -1
			}
			if av > bf {
				return 1
			}
			return 0
		case int64:
			bf := float64(bv)
			if av < bf {
				return -1
			}
			if av > bf {
				return 1
			}
			return 0
		default:
			return 0
		}
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

func toBool(v any) bool {
	b, _ := toBoolNil(v)
	return b
}

// toBoolNil returns (bool-value, true) for non-nil values,
// or (false, false) for nil. Used for SQL three-valued logic.
func toBoolNil(v any) (bool, bool) {
	if v == nil {
		return false, false
	}
	switch b := v.(type) {
	case bool:
		return b, true
	case int32:
		return b != 0, true
	case int64:
		return b != 0, true
	}
	return false, false
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func arithOp(a, b any, intFn func(int64, int64) int64, floatFn func(float64, float64) float64) any {
	switch av := a.(type) {
	case int32:
		switch bv := b.(type) {
		case int32:
			return intFn(int64(av), int64(bv))
		case int64:
			return intFn(int64(av), bv)
		case float64:
			return floatFn(float64(av), bv)
		}
	case int64:
		switch bv := b.(type) {
		case int32:
			return intFn(av, int64(bv))
		case int64:
			return intFn(av, bv)
		case float64:
			return floatFn(float64(av), bv)
		}
	case float64:
		switch bv := b.(type) {
		case int32:
			return floatFn(av, float64(bv))
		case int64:
			return floatFn(av, float64(bv))
		case float64:
			return floatFn(av, bv)
		}
	}
	return nil
}

func exprToString(expr Expr) string {
	switch ex := expr.(type) {
	case *ColumnRefExpr:
		if ex.Table != "" {
			return ex.Table + "." + ex.Name
		}
		return ex.Name
	case *LiteralExpr:
		return fmt.Sprintf("%v", ex.Value)
	case *BinaryExpr:
		return exprToString(ex.Left) + " " + ex.Op + " " + exprToString(ex.Right)
	case *UnaryExpr:
		return ex.Op + exprToString(ex.Operand)
	case *FuncCallExpr:
		var args []string
		for _, a := range ex.Args {
			args = append(args, exprToString(a))
		}
		return ex.Name + "(" + strings.Join(args, ",") + ")"
	case *AggregateFuncExpr:
		var args []string
		for _, a := range ex.Args {
			args = append(args, exprToString(a))
		}
		return ex.Name + "(" + strings.Join(args, ",") + ")"
	case *CastExpr:
		return "CAST(" + exprToString(ex.Expr) + " AS " + ex.Type + ")"
	default:
		return "?"
	}
}

// --- CBO Cost Model ---

// AccessPath represents a possible data access strategy.
type AccessPath struct {
	Type       string  // "full_scan", "pk_range", "pk_point", "index_scan", "index_covering"
	IndexName  string  // name of index used (empty for full_scan)
	MatchCols  int     // number of matched equality columns
	EstRows    int64   // estimated output rows
	EstCost    float64 // estimated cost
	IsCovering bool    // true if covering index scan
}

// defaultSelectivity returns heuristic selectivity for an operator.
func defaultSelectivity(op string) float64 {
	switch {
	case op == "=" || op == "<=>":
		return 0.05
	case op == "!=":
		return 0.9
	case op == "<" || op == "<=" || op == ">" || op == ">=":
		return 0.33
	default:
		return 0.33
	}
}

// colStatsByName returns column stats for the given column name, or nil.
func colStatsByName(stats *catalog.TableStats, name string) *catalog.ColumnStats {
	if stats == nil {
		return nil
	}
	for i := range stats.ColStats {
		if stats.ColStats[i].Name == name {
			return &stats.ColStats[i]
		}
	}
	return nil
}

// estimateSelectivity estimates selectivity for col op val.
func estimateSelectivity(td *catalog.TableDef, colName, op string, val any) float64 {
	if td.Stats == nil {
		return defaultSelectivity(op)
	}
	cs := colStatsByName(td.Stats, colName)
	if cs == nil {
		return defaultSelectivity(op)
	}
	if op == "=" {
		if cs.NDV > 0 {
			return 1.0 / float64(cs.NDV)
		}
		return defaultSelectivity("=")
	}
	// Range selectivity via min/max interpolation.
	if cs.MinVal != nil && cs.MaxVal != nil && val != nil {
		cmp := compareValues(val, cs.MaxVal)
		if cmp >= 0 {
			return 1.0
		}
		cmp = compareValues(val, cs.MinVal)
		if cmp <= 0 {
			return 0.0
		}
		// Linear interpolation (works well for numeric types).
		switch minV := cs.MinVal.(type) {
		case int64:
			if maxV, ok := cs.MaxVal.(int64); ok {
				if v, ok := toInt64(val); ok {
					return float64(v-minV) / float64(maxV-minV)
				}
			}
		case float64:
			if maxV, ok := cs.MaxVal.(float64); ok {
				if v, ok := toFloat64(val); ok {
					return (v - minV) / (maxV - minV)
				}
			}
		}
	}
	return defaultSelectivity(op)
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int:
		return float64(n), true
	}
	return 0, false
}

// estimateExprSelectivity recursively estimates selectivity of an expression.
func estimateExprSelectivity(td *catalog.TableDef, expr Expr) float64 {
	if expr == nil {
		return 1.0
	}
	switch ex := expr.(type) {
	case *BinaryExpr:
		switch ex.Op {
		case "AND":
			return estimateExprSelectivity(td, ex.Left) * estimateExprSelectivity(td, ex.Right)
		case "OR":
			selL := estimateExprSelectivity(td, ex.Left)
			selR := estimateExprSelectivity(td, ex.Right)
			return selL + selR - selL*selR
		case "=", "!=", "<", "<=", ">", ">=":
			colName := ""
			var val any
			if col, ok := ex.Left.(*ColumnRefExpr); ok {
				colName = col.Name
				if lit, ok := ex.Right.(*LiteralExpr); ok {
					val = lit.Value
				}
			} else if col, ok := ex.Right.(*ColumnRefExpr); ok {
				colName = col.Name
				if lit, ok := ex.Left.(*LiteralExpr); ok {
					val = lit.Value
				}
			}
			if colName != "" {
				return estimateSelectivity(td, colName, ex.Op, val)
			}
		}
	case *InExpr:
		if !ex.Not {
			if td.Stats != nil {
				if col, ok := ex.Expr.(*ColumnRefExpr); ok {
					cs := colStatsByName(td.Stats, col.Name)
					if cs != nil && cs.NDV > 0 {
						return float64(len(ex.Values)) / float64(cs.NDV)
					}
				}
			}
			return 0.05 * float64(len(ex.Values))
		}
		return 0.9
	case *BetweenExpr:
		selLow := estimateExprSelectivity(td, &BinaryExpr{Op: ">=", Left: ex.Expr, Right: ex.Low})
		selHigh := estimateExprSelectivity(td, &BinaryExpr{Op: "<=", Left: ex.Expr, Right: ex.High})
		return selLow * selHigh
	case *IsNullExpr:
		if !ex.Not {
			if col, ok := ex.Expr.(*ColumnRefExpr); ok {
				cs := colStatsByName(td.Stats, col.Name)
				if cs != nil && td.Stats.RowCount > 0 {
					return float64(cs.NullCnt) / float64(td.Stats.RowCount)
				}
			}
			return 0.1
		}
		return 0.9
	case *LikeExpr:
		return 0.1
	}
	return 0.33
}

// estimateWHERECardinality estimates the number of rows matching a WHERE clause.
func estimateWHERECardinality(td *catalog.TableDef, where Expr, totalRows int64) int64 {
	if where == nil {
		return totalRows
	}
	sel := estimateExprSelectivity(td, where)
	est := float64(totalRows) * sel
	if est < 1 {
		if totalRows > 0 {
			return 1
		}
		return 0
	}
	return int64(est)
}

// Cost model constants
const (
	costSeqIO       = 1.0   // cost of a sequential I/O (read one page)
	costRandIO      = 4.0   // cost of a random I/O (seek + read)
	costCPURow      = 0.01  // cost of evaluating WHERE on one row
	costCPUIndexRow = 0.005 // cost per index entry scan
	costTableLookup = 5.0   // cost of one table lookup from index (random I/O + decode)
	rowsPerPage     = 100   // estimated rows per data page
)

// estimateAccessPaths enumerates possible access paths for a single-table query.
func (e *Executor) estimateAccessPaths(td *catalog.TableDef, s *SelectStmt) []AccessPath {
	var paths []AccessPath
	totalRows := int64(1000) // default if no stats
	if td.Stats != nil {
		totalRows = td.Stats.RowCount
	}
	estRows := estimateWHERECardinality(td, s.Where, totalRows)

	// 1. Full scan: cost = I/O pages + CPU per row
	pages := float64(totalRows) / rowsPerPage
	if pages < 1 {
		pages = 1
	}
	fullScanCost := pages*costSeqIO + float64(totalRows)*costCPURow
	paths = append(paths, AccessPath{
		Type:    "full_scan",
		EstRows: estRows,
		EstCost: fullScanCost,
	})

	if s.Where == nil {
		return paths
	}

	eqMap := e.collectEqualities(s.Where)

	// 2. PK point/range
	pkEqCount := 0
	for _, pkIdx := range td.PKCols {
		if _, ok := eqMap[td.Columns[pkIdx].Name]; ok {
			pkEqCount++
		}
	}

	// Also check for IN-on-PK: WHERE col IN (v1, v2, ...) where col is the PK.
	inOnPK, hasINOnPK := e.findINOnPK(s.Where, td)

	if pkEqCount == len(td.PKCols) && len(td.PKCols) > 0 {
		// Full PK match — point lookup. Each lookup = 1 random I/O.
		inCount := int64(1)
		if inExpr, ok := s.Where.(*InExpr); ok && !inExpr.Not {
			inCount = int64(len(inExpr.Values))
		}
		if inCount == 0 {
			inCount = 1
		}
		// AND chain with all PK cols = single point lookup
		if inCount == 1 {
			// Check if it's really an AND chain of PK equalities (not IN)
			if _, ok := s.Where.(*InExpr); !ok {
				inCount = 1
			}
		}
		pkPointCost := float64(inCount) * costRandIO
		// Even for small N, PK point is cheap. But for very large N, table scan wins.
		paths = append(paths, AccessPath{
			Type:      "pk_point",
			IndexName: "PRIMARY",
			MatchCols: pkEqCount,
			EstRows:   inCount,
			EstCost:   pkPointCost,
		})
	} else if hasINOnPK {
		// IN on all PK columns — multiple point lookups.
		inCount := int64(len(inOnPK.Values))
		if inCount == 0 {
			inCount = 1
		}
		pkPointCost := float64(inCount) * costRandIO
		paths = append(paths, AccessPath{
			Type:      "pk_point",
			IndexName: "PRIMARY",
			MatchCols: len(td.PKCols),
			EstRows:   inCount,
			EstCost:   pkPointCost,
		})
	} else if pkEqCount > 0 {
		// Partial PK match — range scan.
		pkSel := 1.0
		for i := 0; i < pkEqCount; i++ {
			colName := td.Columns[td.PKCols[i]].Name
			if cs := colStatsByName(td.Stats, colName); cs != nil && cs.NDV > 0 {
				pkSel *= 1.0 / float64(cs.NDV)
			} else {
				pkSel *= 0.05
			}
		}
		pkEstRows := int64(float64(totalRows) * pkSel)
		if pkEstRows < 1 {
			pkEstRows = 1
		}
		// Range scan: sequential I/O for the range + CPU per row
		pkPages := float64(pkEstRows) / rowsPerPage
		if pkPages < 1 {
			pkPages = 1
		}
		pkRangeCost := pkPages*costSeqIO + float64(pkEstRows)*costCPURow
		paths = append(paths, AccessPath{
			Type:      "pk_range",
			IndexName: "PRIMARY",
			MatchCols: pkEqCount,
			EstRows:   pkEstRows,
			EstCost:   pkRangeCost,
		})
	}

	// 3. Secondary index scans
	for i := range td.Indexes {
		idx := &td.Indexes[i]
		idxEqCols := 0
		for _, colName := range idx.Columns {
			if _, ok := eqMap[colName]; ok {
				idxEqCols++
			} else {
				break
			}
		}
		if idxEqCols == 0 {
			continue
		}

		idxSel := 1.0
		for j := 0; j < idxEqCols; j++ {
			colName := idx.Columns[j]
			if cs := colStatsByName(td.Stats, colName); cs != nil && cs.NDV > 0 {
				idxSel *= 1.0 / float64(cs.NDV)
			} else {
				idxSel *= 0.05
			}
		}
		idxEstRows := int64(float64(totalRows) * idxSel)
		if idxEstRows < 1 {
			idxEstRows = 1
		}

		isCovering := e.isCoveringIndex(td, idx, s)
		var cost float64
		if isCovering {
			// Covering: only scan index, no table lookups
			idxPages := float64(idxEstRows) / rowsPerPage
			if idxPages < 1 {
				idxPages = 1
			}
			cost = idxPages*costSeqIO + float64(idxEstRows)*costCPUIndexRow
		} else {
			// Non-covering: scan index + random I/O per matching row for table lookup
			cost = float64(idxEstRows)*costCPUIndexRow + float64(idxEstRows)*costTableLookup
		}

		apType := "index_scan"
		if isCovering {
			apType = "index_covering"
		}
		paths = append(paths, AccessPath{
			Type:       apType,
			IndexName:  idx.Name,
			MatchCols:  idxEqCols,
			EstRows:    idxEstRows,
			EstCost:    cost,
			IsCovering: isCovering,
		})
	}

	return paths
}

// selectBestPath chooses the cheapest access path.
func selectBestPath(paths []AccessPath) AccessPath {
	if len(paths) == 0 {
		return AccessPath{Type: "full_scan"}
	}
	best := paths[0]
	for _, p := range paths[1:] {
		if p.EstCost < best.EstCost {
			best = p
		}
	}
	return best
}

// ─── Join optimization: types, helpers, cost model ─────────────────────

// joinEquiKey represents one equality column pair in an equi-join.
type joinEquiKey struct {
	leftColIdx  int // left table column index
	rightColIdx int // right table column index
	leftName    string
	rightName   string
}

// joinPlan holds the optimizer's decisions for executing a join.
type joinPlan struct {
	method       string // "hash_join" | "nested_loop"
	buildSide    TableRef
	probeSide    TableRef
	buildTd      *catalog.TableDef
	probeTd      *catalog.TableDef
	buildAlias   string
	probeAlias   string
	equiKeys     []joinEquiKey // build side key column indices
	probeKeyIdx  []int         // probe side column indices
	residualOn   Expr          // non-equi residual from ON
	leftWhere    Expr          // WHERE predicates for left table
	rightWhere   Expr          // WHERE predicates for right table
	remainWhere  Expr          // cross-table WHERE (evaluated after join)
	swapped      bool          // true if INNER join swapped left/right
	estCost      float64
	estRows      int64
	estBuildRows int64
	estProbeRows int64
}

// flatTableEntry represents one table in a flattened join tree.
type flatTableEntry struct {
	tableName string            // actual table name
	alias     string            // alias or table name if no alias
	td        *catalog.TableDef // table definition
	colOffset int               // starting column offset in the joined row
	colCount  int               // len(td.Columns)
}

// countTables counts the number of leaf tables in a TableRef tree.
func countTables(ref TableRef) int {
	switch r := ref.(type) {
	case *SimpleTableRef:
		return 1
	case *JoinTableRef:
		return countTables(r.Left) + countTables(r.Right)
	}
	return 0
}

// flattenJoinTree flattens a nested JoinTableRef tree into an ordered list of
// flatTableEntry with computed column offsets.
func (e *Executor) flattenJoinTree(ref TableRef) ([]flatTableEntry, error) {
	var entries []flatTableEntry
	e.walkJoinTree(ref, &entries)

	// Compute column offsets.
	offset := 0
	for i := range entries {
		entries[i].colOffset = offset
		entries[i].colCount = len(entries[i].td.Columns)
		offset += entries[i].colCount
	}
	return entries, nil
}

func (e *Executor) walkJoinTree(ref TableRef, entries *[]flatTableEntry) {
	switch r := ref.(type) {
	case *SimpleTableRef:
		td, err := e.cat.GetTable(e.dbName, r.Table)
		if err != nil {
			return
		}
		alias := r.Alias
		if alias == "" {
			alias = r.Table
		}
		*entries = append(*entries, flatTableEntry{
			tableName: r.Table,
			alias:     alias,
			td:        td,
		})
	case *JoinTableRef:
		e.walkJoinTree(r.Left, entries)
		e.walkJoinTree(r.Right, entries)
	}
}

// resolveColumnOffset finds the absolute column index in a joined row.
func resolveColumnOffset(col *ColumnRefExpr, tables []flatTableEntry) (int, error) {
	if col.Table != "" {
		for _, entry := range tables {
			if entry.tableName == col.Table || entry.alias == col.Table {
				idx := entry.td.ColumnIndex(col.Name)
				if idx >= 0 {
					return entry.colOffset + idx, nil
				}
			}
		}
		return -1, fmt.Errorf("column %s.%s not found in join", col.Table, col.Name)
	}
	// Unqualified: search all tables.
	for _, entry := range tables {
		idx := entry.td.ColumnIndex(col.Name)
		if idx >= 0 {
			return entry.colOffset + idx, nil
		}
	}
	return -1, fmt.Errorf("column %s not found in any join table", col.Name)
}

// evalExprMultiJoin evaluates an expression over a fully-joined row using the
// flat table list for column resolution.
func (e *Executor) evalExprMultiJoin(expr Expr, tables []flatTableEntry, joinedRow []any) any {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		idx, err := resolveColumnOffset(ex, tables)
		if err != nil || idx < 0 || idx >= len(joinedRow) {
			return nil
		}
		return joinedRow[idx]
	case *BinaryExpr:
		left := e.evalExprMultiJoin(ex.Left, tables, joinedRow)
		right := e.evalExprMultiJoin(ex.Right, tables, joinedRow)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprMultiJoin(ex.Operand, tables, joinedRow)
		return e.evalUnaryOp(ex.Op, operand)
	case *NullExpr:
		return nil
	case *IsNullExpr:
		v := e.evalExprMultiJoin(ex.Expr, tables, joinedRow)
		if ex.Not {
			return v != nil
		}
		return v == nil
	case *InExpr:
		return e.evalInExprMultiJoin(tables, ex, joinedRow)
	case *LikeExpr:
		val := e.evalExprMultiJoin(ex.Expr, tables, joinedRow)
		pattern := e.evalExprMultiJoin(ex.Pattern, tables, joinedRow)
		result := e.evalLike(val, pattern)
		if ex.Not {
			return !result
		}
		return result
	case *BetweenExpr:
		val := e.evalExprMultiJoin(ex.Expr, tables, joinedRow)
		low := e.evalExprMultiJoin(ex.Low, tables, joinedRow)
		high := e.evalExprMultiJoin(ex.High, tables, joinedRow)
		if val == nil || low == nil || high == nil {
			return nil
		}
		cmpLow := compareValues(val, low)
		cmpHigh := compareValues(val, high)
		result := cmpLow >= 0 && cmpHigh <= 0
		if ex.Not {
			result = !result
		}
		return result
	case *CaseExpr:
		for _, w := range ex.Whens {
			var cond any
			if ex.Value != nil {
				val := e.evalExprMultiJoin(ex.Value, tables, joinedRow)
				cmp := e.evalExprMultiJoin(w.Cond, tables, joinedRow)
				if val == nil || cmp == nil {
					cond = false
				} else {
					cond = compareValues(val, cmp) == 0
				}
			} else {
				cond = e.evalExprMultiJoin(w.Cond, tables, joinedRow)
			}
			if b, ok := cond.(bool); ok && b {
				return e.evalExprMultiJoin(w.Result, tables, joinedRow)
			}
		}
		if ex.Else != nil {
			return e.evalExprMultiJoin(ex.Else, tables, joinedRow)
		}
		return nil
	case *FuncCallExpr:
		return e.evalFuncCallMultiJoin(tables, ex, joinedRow)
	case *CastExpr:
		v := e.evalExprMultiJoin(ex.Expr, tables, joinedRow)
		return castValue(v, ex.Type)
	default:
		return nil
	}
}

func (e *Executor) evalInExprMultiJoin(tables []flatTableEntry, expr *InExpr, joinedRow []any) bool {
	if expr.Empty {
		return expr.Not
	}
	val := e.evalExprMultiJoin(expr.Expr, tables, joinedRow)
	hasNull := val == nil
	for _, v := range expr.Values {
		if sq, ok := v.(*SubqueryExpr); ok {
			result := e.execSubquery(sq.Query)
			if rs, ok := result.(*SelectResult); ok {
				if len(rs.Columns) > 1 {
					panic(evalError{fmt.Errorf("sub-select returns %d columns - expected 1", len(rs.Columns))})
				}
				for _, row := range rs.Rows {
					if len(row) > 0 && compareValues(val, row[0]) == 0 {
						return !expr.Not
					}
				}
			}
		} else {
			ev := e.evalExprMultiJoin(v, tables, joinedRow)
			if ev == nil {
				hasNull = true
				continue
			}
			if val != nil && compareValues(val, ev) == 0 {
				return !expr.Not
			}
		}
	}
	if hasNull {
		return false
	}
	return expr.Not
}

func (e *Executor) evalFuncCallMultiJoin(tables []flatTableEntry, f *FuncCallExpr, joinedRow []any) any {
	switch strings.ToUpper(f.Name) {
	case "COALESCE":
		for _, arg := range f.Args {
			v := e.evalExprMultiJoin(arg, tables, joinedRow)
			if v != nil {
				return v
			}
		}
		return nil
	case "IFNULL":
		if len(f.Args) >= 2 {
			v := e.evalExprMultiJoin(f.Args[0], tables, joinedRow)
			if v == nil {
				return e.evalExprMultiJoin(f.Args[1], tables, joinedRow)
			}
			return v
		}
	}
	return nil
}

// evalWhereMultiJoin evaluates a WHERE expression and returns a boolean.
func (e *Executor) evalWhereMultiJoin(expr Expr, tables []flatTableEntry, joinedRow []any) bool {
	result := e.evalExprMultiJoin(expr, tables, joinedRow)
	if b, ok := result.(bool); ok {
		return b
	}
	if result == nil {
		return false
	}
	return true
}

// flattenAND flattens a tree of AND-connected expressions into a flat list.
func flattenAND(expr Expr) []Expr {
	if expr == nil {
		return nil
	}
	bin, ok := expr.(*BinaryExpr)
	if !ok || bin.Op != "AND" {
		return []Expr{expr}
	}
	return append(flattenAND(bin.Left), flattenAND(bin.Right)...)
}

// colBelongsTo checks whether a column reference belongs to a given table.
func colBelongsTo(col *ColumnRefExpr, td *catalog.TableDef, alias string) bool {
	if col.Table != "" {
		return col.Table == td.Name || col.Table == alias
	}
	// No table prefix: check if column name exists in table def.
	return td.ColumnIndex(col.Name) >= 0
}

// andExpr combines two expressions with AND, handling nil.
func andExpr(a, b Expr) Expr {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &BinaryExpr{Op: "AND", Left: a, Right: b}
}

// collectTableRefs gathers all table prefixes from ColumnRefExprs in an expression.
func collectTableRefs(expr Expr) map[string]bool {
	refs := make(map[string]bool)
	var walk func(Expr)
	walk = func(e Expr) {
		if e == nil {
			return
		}
		switch ex := e.(type) {
		case *ColumnRefExpr:
			if ex.Table != "" {
				refs[ex.Table] = true
			} else {
				refs[""] = true // unqualified column
			}
		case *BinaryExpr:
			walk(ex.Left)
			walk(ex.Right)
		case *UnaryExpr:
			walk(ex.Operand)
		case *BetweenExpr:
			walk(ex.Expr)
			walk(ex.Low)
			walk(ex.High)
		case *InExpr:
			walk(ex.Expr)
			for _, v := range ex.Values {
				walk(v)
			}
		case *IsNullExpr:
			walk(ex.Expr)
		case *LikeExpr:
			walk(ex.Expr)
			walk(ex.Pattern)
		case *FuncCallExpr:
			for _, a := range ex.Args {
				walk(a)
			}
		}
	}
	walk(expr)
	return refs
}

// extractEquiJoinKeys extracts equality join conditions from an ON expression.
// Returns equi-join keys and a residual expression for non-equi conditions.
func extractEquiJoinKeys(on Expr, leftTd, rightTd *catalog.TableDef, leftAlias, rightAlias string) (keys []joinEquiKey, residual Expr) {
	if on == nil {
		return nil, nil
	}
	bin, ok := on.(*BinaryExpr)
	if !ok {
		return nil, on
	}
	if bin.Op == "AND" {
		leftKeys, leftRes := extractEquiJoinKeys(bin.Left, leftTd, rightTd, leftAlias, rightAlias)
		rightKeys, rightRes := extractEquiJoinKeys(bin.Right, leftTd, rightTd, leftAlias, rightAlias)
		keys = append(leftKeys, rightKeys...)
		residual = andExpr(leftRes, rightRes)
		return keys, residual
	}
	if bin.Op == "=" {
		lCol, lOk := bin.Left.(*ColumnRefExpr)
		rCol, rOk := bin.Right.(*ColumnRefExpr)
		if lOk && rOk {
			lLeft := colBelongsTo(lCol, leftTd, leftAlias)
			lRight := colBelongsTo(lCol, rightTd, rightAlias)
			rLeft := colBelongsTo(rCol, leftTd, leftAlias)
			rRight := colBelongsTo(rCol, rightTd, rightAlias)

			if (lLeft && rRight) && !(lRight && rLeft) {
				li := leftTd.ColumnIndex(lCol.Name)
				ri := rightTd.ColumnIndex(rCol.Name)
				if li >= 0 && ri >= 0 {
					return []joinEquiKey{{leftColIdx: li, rightColIdx: ri, leftName: lCol.Name, rightName: rCol.Name}}, nil
				}
			}
			if (rLeft && lRight) && !(rRight && lLeft) {
				li := leftTd.ColumnIndex(rCol.Name)
				ri := rightTd.ColumnIndex(lCol.Name)
				if li >= 0 && ri >= 0 {
					return []joinEquiKey{{leftColIdx: li, rightColIdx: ri, leftName: rCol.Name, rightName: lCol.Name}}, nil
				}
			}
		}
	}
	return nil, on
}

// splitWhereByTable splits a WHERE expression into per-table and cross-table parts.
func splitWhereByTable(where Expr, leftTd, rightTd *catalog.TableDef, leftAlias, rightAlias string) (leftWhere, rightWhere, remainWhere Expr) {
	if where == nil {
		return nil, nil, nil
	}
	bin, ok := where.(*BinaryExpr)
	if ok && bin.Op == "AND" {
		lw, rw, remw := splitWhereByTable(bin.Left, leftTd, rightTd, leftAlias, rightAlias)
		lw2, rw2, remw2 := splitWhereByTable(bin.Right, leftTd, rightTd, leftAlias, rightAlias)
		return andExpr(lw, lw2), andExpr(rw, rw2), andExpr(remw, remw2)
	}

	refs := collectTableRefs(where)
	hasLeft := false
	hasRight := false
	for ref := range refs {
		if ref == "" {
			// Unqualified column: check both tables.
			// These go to remainWhere since we can't safely attribute them.
			hasLeft = true
			hasRight = true
			continue
		}
		if ref == leftTd.Name || ref == leftAlias {
			hasLeft = true
		}
		if ref == rightTd.Name || ref == rightAlias {
			hasRight = true
		}
	}

	// If the expression only references column names without table prefix,
	// try to determine if all columns belong to one table.
	if refs[""] && len(refs) == 1 {
		allLeft := allColsBelongTo(where, leftTd)
		allRight := allColsBelongTo(where, rightTd)
		if allLeft && !allRight {
			return where, nil, nil
		}
		if allRight && !allLeft {
			return nil, where, nil
		}
		// Both tables have matching columns or ambiguous → remain
		return nil, nil, where
	}

	if hasLeft && !hasRight {
		return where, nil, nil
	}
	if hasRight && !hasLeft {
		return nil, where, nil
	}
	return nil, nil, where
}

// allColsBelongTo checks if all ColumnRefExprs (without table prefix) in an expression
// can be resolved in the given table definition.
func allColsBelongTo(expr Expr, td *catalog.TableDef) bool {
	switch ex := expr.(type) {
	case *ColumnRefExpr:
		if ex.Table != "" {
			return ex.Table == td.Name
		}
		return td.ColumnIndex(ex.Name) >= 0
	case *BinaryExpr:
		return allColsBelongTo(ex.Left, td) && allColsBelongTo(ex.Right, td)
	case *UnaryExpr:
		return allColsBelongTo(ex.Operand, td)
	case *BetweenExpr:
		return allColsBelongTo(ex.Expr, td) && allColsBelongTo(ex.Low, td) && allColsBelongTo(ex.High, td)
	case *InExpr:
		if !allColsBelongTo(ex.Expr, td) {
			return false
		}
		for _, v := range ex.Values {
			if !allColsBelongTo(v, td) {
				return false
			}
		}
		return true
	case *IsNullExpr:
		return allColsBelongTo(ex.Expr, td)
	case *LikeExpr:
		return allColsBelongTo(ex.Expr, td) && allColsBelongTo(ex.Pattern, td)
	case *FuncCallExpr:
		for _, a := range ex.Args {
			if !allColsBelongTo(a, td) {
				return false
			}
		}
		return true
	case *LiteralExpr:
		return true
	case *NullExpr:
		return true
	}
	return true
}

// splitWhereNTables splits a WHERE expression into per-table predicates and
// cross-table (remain) predicates for N tables.
func splitWhereNTables(where Expr, tables []flatTableEntry) ([]Expr, Expr) {
	if where == nil {
		return make([]Expr, len(tables)), nil
	}
	conjuncts := flattenAND(where)
	tableWheres := make([]Expr, len(tables))
	var remain []Expr

	for _, conj := range conjuncts {
		assigned := false
		for i, entry := range tables {
			if allColsBelongTo(conj, entry.td) {
				// Check that no other table also claims it.
				claimed := false
				for j, other := range tables {
					if j != i && allColsBelongTo(conj, other.td) {
						claimed = true
						break
					}
				}
				if !claimed {
					tableWheres[i] = andExpr(tableWheres[i], conj)
					assigned = true
					break
				}
			}
		}
		if !assigned {
			remain = append(remain, conj)
		}
	}

	var remainWhere Expr
	for _, r := range remain {
		remainWhere = andExpr(remainWhere, r)
	}
	return tableWheres, remainWhere
}

// collectTableRows collects rows from a single table with optional WHERE pushdown.
func (e *Executor) collectTableRows(t *txn.Txn, entry flatTableEntry, where Expr) ([][]any, error) {
	storedTd, err := e.cat.GetTable(e.dbName, entry.tableName)
	if err != nil {
		return nil, err
	}
	treeKey := storedTd.DataFile()
	pkCols := storedTd.PrimaryKeyColumns()

	if where == nil {
		// Full scan, no filter.
		var rows [][]any
		t.Scan(treeKey, pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
			vals, _ := storage.DecodeRow(rowData, storedTd.Columns)
			rows = append(rows, vals)
			return true
		})
		return rows, nil
	}

	// Scan with WHERE pushdown.
	var rows [][]any
	t.Scan(treeKey, pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, storedTd.Columns)
		if e.evalWhere(storedTd, where, vals) {
			rows = append(rows, vals)
		}
		return true
	})
	return rows, nil
}

// ─── Multi-table join execution ─────────────────────────────────────────

func (e *Executor) execMultiTableJoin(t *txn.Txn, s *SelectStmt, ref *JoinTableRef) (any, error) {
	// Step 1: Flatten the join tree.
	tables, err := e.flattenJoinTree(ref)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no tables in join")
	}

	// Step 1.5: Reorder tables using greedy on equi-join graph for hash join efficiency.
	// Build adjacency by table name for flexibility.
	conjuncts := flattenAND(s.Where)
	nameToIdx := make(map[string]int, len(tables))
	for i, entry := range tables {
		nameToIdx[entry.tableName] = i
	}
	adjByName := make(map[string][]string) // tableName -> list of neighbor tableNames
	for _, conj := range conjuncts {
		bin, ok := conj.(*BinaryExpr)
		if !ok || bin.Op != "=" {
			continue
		}
		leftCol, lok := bin.Left.(*ColumnRefExpr)
		rightCol, rok := bin.Right.(*ColumnRefExpr)
		if !lok || !rok {
			continue
		}
		li := resolveTableIndex(leftCol, tables)
		ri := resolveTableIndex(rightCol, tables)
		if li >= 0 && ri >= 0 && li != ri {
			ln := tables[li].tableName
			rn := tables[ri].tableName
			adjByName[ln] = append(adjByName[ln], rn)
			adjByName[rn] = append(adjByName[rn], ln)
		}
	}

	if len(adjByName) > 0 {
		// Pick start table with most connections.
		startName := tables[0].tableName
		maxConn := len(adjByName[startName])
		for _, entry := range tables {
			if len(adjByName[entry.tableName]) > maxConn {
				maxConn = len(adjByName[entry.tableName])
				startName = entry.tableName
			}
		}

		if maxConn > 0 {
			// Greedy: always pick the unvisited table with the most connections to visited set.
			visited := make(map[string]bool, len(tables))
			var orderNames []string
			visited[startName] = true
			orderNames = append(orderNames, startName)
			for len(orderNames) < len(tables) {
				bestName := ""
				bestScore := 0
				for _, entry := range tables {
					n := entry.tableName
					if visited[n] {
						continue
					}
					score := 0
					for _, nb := range adjByName[n] {
						if visited[nb] {
							score++
						}
					}
					if score > bestScore || (bestName == "" && bestScore == 0) {
						bestScore = score
						bestName = n
					}
				}
				orderNames = append(orderNames, bestName)
				visited[bestName] = true
			}

			// Build reordered tables slice from orderNames.
			nameToEntry := make(map[string]flatTableEntry, len(tables))
			for _, entry := range tables {
				nameToEntry[entry.tableName] = entry
			}
			reordered := make([]flatTableEntry, len(tables))
			for i, name := range orderNames {
				reordered[i] = nameToEntry[name]
			}
			offset := 0
			for i := range reordered {
				reordered[i].colOffset = offset
				offset += reordered[i].colCount
			}
			tables = reordered
		}
	}

	// Step 2: Split WHERE into per-table and cross-table predicates.
	tableWheres, remainWhere := splitWhereNTables(s.Where, tables)

	// Step 3: Collect rows per table with WHERE pushdown.
	tableRows := make([][][]any, len(tables))
	for i, entry := range tables {
		tableRows[i], err = e.collectTableRows(t, entry, tableWheres[i])
		if err != nil {
			return nil, err
		}
	}

	// Step 4: Split remainWhere into individual conjuncts for early filtering.
	var remainConjuncts []Expr
	if remainWhere != nil {
		remainConjuncts = flattenAND(remainWhere)
	}

	// Step 5: Left-deep join with hash join optimization.
	resultRows := tableRows[0]
	joinedSet := map[int]bool{0: true}

	// Debug: detect slow queries

	for i := 1; i < len(tables); i++ {
		newRows := tableRows[i]

		// Determine which remain conjuncts can be applied now (all referenced tables joined).
		var applicableConjuncts []Expr
		var deferredConjuncts []Expr
		for _, conj := range remainConjuncts {
			refs := collectConjunctTableRefs(conj, tables)
			allJoined := true
			for idx := range refs {
				if idx <= i && !joinedSet[idx] && idx != i {
					allJoined = false
					break
				}
				if idx > i {
					allJoined = false
					break
				}
			}
			if allJoined {
				applicableConjuncts = append(applicableConjuncts, conj)
			} else {
				deferredConjuncts = append(deferredConjuncts, conj)
			}
		}

		// Extract equi-join keys for hash join.
		buildLocalOffsets, probeOffsets, residualConjs := extractEquiKeysForTable(applicableConjuncts, i, tables)

		var nextResult [][]any

		if len(buildLocalOffsets) > 0 {
			// HASH JOIN: build hash table on newRows (table i).
			hashTable := make(map[string][][]any, len(newRows))
			for _, newRow := range newRows {
				key, skip := hashKeyMultiTable(newRow, buildLocalOffsets)
				if skip {
					continue
				}
				hashTable[key] = append(hashTable[key], newRow)
			}

			// Probe: for each accumulated row, look up matching new rows.
			for _, existingRow := range resultRows {
				key, skip := hashKeyMultiTable(existingRow, probeOffsets)
				if skip {
					continue
				}
				matches := hashTable[key]
				for _, newRow := range matches {
					joined := make([]any, 0, len(existingRow)+len(newRow))
					joined = append(joined, existingRow...)
					joined = append(joined, newRow...)

					// Apply residual conjuncts (non-equi conditions).
					pass := true
					for _, conj := range residualConjs {
						if !e.evalWhereMultiJoin(conj, tables, joined) {
							pass = false
							break
						}
					}
					if pass {
						nextResult = append(nextResult, joined)
					}
				}
			}
		} else {
			// FALLBACK: Nested loop join (no equi-join condition available).
			for _, existingRow := range resultRows {
				for _, newRow := range newRows {
					joined := make([]any, 0, len(existingRow)+len(newRow))
					joined = append(joined, existingRow...)
					joined = append(joined, newRow...)

					pass := true
					for _, conj := range applicableConjuncts {
						if !e.evalWhereMultiJoin(conj, tables, joined) {
							pass = false
							break
						}
					}
					if pass {
						nextResult = append(nextResult, joined)
					}
				}
			}
		}

		resultRows = nextResult
		joinedSet[i] = true
		remainConjuncts = deferredConjuncts
	}

	// Step 6: Project result columns.
	resultRows, colNames := e.projectMultiJoinResult(s, tables, resultRows)

	// Apply ORDER BY.
	if len(s.OrderBy) > 0 {
		e.sortRowsWithFields(resultRows, colNames, s.OrderBy, nil)
	}
	// Apply LIMIT.
	if s.Limit != nil && *s.Limit < len(resultRows) {
		resultRows = resultRows[:*s.Limit]
	}
	// Apply DISTINCT.
	if s.Distinct {
		resultRows = dedupRows(resultRows)
	}

	return &SelectResult{Columns: colNames, Rows: resultRows}, nil
}

// collectConjunctTableRefs returns the set of table indices referenced by an expression.
func collectConjunctTableRefs(expr Expr, tables []flatTableEntry) map[int]bool {
	refs := make(map[int]bool)
	collectTableRefsInExpr(expr, tables, refs)
	return refs
}

func collectTableRefsInExpr(expr Expr, tables []flatTableEntry, refs map[int]bool) {
	switch ex := expr.(type) {
	case *ColumnRefExpr:
		if ex.Table != "" {
			for i, entry := range tables {
				if entry.tableName == ex.Table || entry.alias == ex.Table {
					if entry.td.ColumnIndex(ex.Name) >= 0 {
						refs[i] = true
						return
					}
				}
			}
		} else {
			for i, entry := range tables {
				if entry.td.ColumnIndex(ex.Name) >= 0 {
					refs[i] = true
					return
				}
			}
		}
	case *BinaryExpr:
		collectTableRefsInExpr(ex.Left, tables, refs)
		collectTableRefsInExpr(ex.Right, tables, refs)
	case *UnaryExpr:
		collectTableRefsInExpr(ex.Operand, tables, refs)
	case *InExpr:
		collectTableRefsInExpr(ex.Expr, tables, refs)
		for _, v := range ex.Values {
			collectTableRefsInExpr(v, tables, refs)
		}
	case *BetweenExpr:
		collectTableRefsInExpr(ex.Expr, tables, refs)
		collectTableRefsInExpr(ex.Low, tables, refs)
		collectTableRefsInExpr(ex.High, tables, refs)
	case *IsNullExpr:
		collectTableRefsInExpr(ex.Expr, tables, refs)
	case *LikeExpr:
		collectTableRefsInExpr(ex.Expr, tables, refs)
		collectTableRefsInExpr(ex.Pattern, tables, refs)
	case *FuncCallExpr:
		for _, a := range ex.Args {
			collectTableRefsInExpr(a, tables, refs)
		}
	case *CaseExpr:
		if ex.Value != nil {
			collectTableRefsInExpr(ex.Value, tables, refs)
		}
		for _, w := range ex.Whens {
			collectTableRefsInExpr(w.Cond, tables, refs)
			collectTableRefsInExpr(w.Result, tables, refs)
		}
		if ex.Else != nil {
			collectTableRefsInExpr(ex.Else, tables, refs)
		}
	}
}

// resolveTableIndex returns the index in tables that a ColumnRefExpr belongs to, or -1.
func resolveTableIndex(col *ColumnRefExpr, tables []flatTableEntry) int {
	if col.Table != "" {
		for i, entry := range tables {
			if entry.tableName == col.Table || entry.alias == col.Table {
				if entry.td.ColumnIndex(col.Name) >= 0 {
					return i
				}
			}
		}
	} else {
		for i, entry := range tables {
			if entry.td.ColumnIndex(col.Name) >= 0 {
				return i
			}
		}
	}
	return -1
}

// extractEquiKeysForTable splits applicable conjuncts into equi-join keys
// (buildLocalOffsets = local column indices within the new table's row,
//
//	probeOffsets = absolute column offsets in the accumulated joined row)
//
// and residual conjuncts (non-equi or same-table conditions).
func extractEquiKeysForTable(conjuncts []Expr, tableIdx int, tables []flatTableEntry) (
	buildLocalOffsets []int, probeOffsets []int, residualConjuncts []Expr) {

	for _, conj := range conjuncts {
		bin, ok := conj.(*BinaryExpr)
		if !ok || bin.Op != "=" {
			residualConjuncts = append(residualConjuncts, conj)
			continue
		}
		leftCol, leftOk := bin.Left.(*ColumnRefExpr)
		rightCol, rightOk := bin.Right.(*ColumnRefExpr)
		if !leftOk || !rightOk {
			residualConjuncts = append(residualConjuncts, conj)
			continue
		}

		leftIdx := resolveTableIndex(leftCol, tables)
		rightIdx := resolveTableIndex(rightCol, tables)
		if leftIdx < 0 || rightIdx < 0 || leftIdx == rightIdx {
			residualConjuncts = append(residualConjuncts, conj)
			continue
		}

		var buildCol, probeCol *ColumnRefExpr
		if leftIdx == tableIdx && rightIdx < tableIdx {
			buildCol = leftCol
			probeCol = rightCol
		} else if rightIdx == tableIdx && leftIdx < tableIdx {
			buildCol = rightCol
			probeCol = leftCol
		} else {
			residualConjuncts = append(residualConjuncts, conj)
			continue
		}

		// Build offset: local index within the single table row.
		buildLocalOff := tables[tableIdx].td.ColumnIndex(buildCol.Name)
		if buildLocalOff < 0 {
			residualConjuncts = append(residualConjuncts, conj)
			continue
		}
		// Probe offset: absolute index in the accumulated joined row.
		probeOff, err := resolveColumnOffset(probeCol, tables)
		if err != nil {
			residualConjuncts = append(residualConjuncts, conj)
			continue
		}
		buildLocalOffsets = append(buildLocalOffsets, buildLocalOff)
		probeOffsets = append(probeOffsets, probeOff)
	}
	return
}

// hashKeyMultiTable extracts a hash key string from row values at the given offsets.
// Returns ("", true) if any value is nil (skip).
func hashKeyMultiTable(row []any, offsets []int) (string, bool) {
	var buf strings.Builder
	for _, off := range offsets {
		if off >= len(row) {
			return "", true
		}
		v := row[off]
		if v == nil {
			return "", true
		}
		fmt.Fprintf(&buf, "%v\x00", v)
	}
	return buf.String(), false
}

func (e *Executor) projectMultiJoinResult(s *SelectStmt, tables []flatTableEntry, rows [][]any) ([][]any, []string) {
	if s.SelectAll {
		// Return all non-hidden columns with qualified names.
		colNames := make([]string, 0)
		// Build list of column offsets to include.
		var includeIdx []int
		for _, entry := range tables {
			for i, col := range entry.td.Columns {
				if col.Hidden {
					continue
				}
				colNames = append(colNames, entry.alias+"."+col.Name)
				includeIdx = append(includeIdx, entry.colOffset+i)
			}
		}
		// Project rows to only include non-hidden columns.
		projected := make([][]any, len(rows))
		for i, row := range rows {
			prow := make([]any, len(includeIdx))
			for j, idx := range includeIdx {
				prow[j] = row[idx]
			}
			projected[i] = prow
		}
		return projected, colNames
	}

	// Build projection descriptors from Fields.
	type projField struct {
		colIdx int  // absolute index in joined row, -1 for expression
		expr   Expr // expression to evaluate
		name   string
	}
	var fields []projField

	if len(s.Fields) > 0 {
		for _, f := range s.Fields {
			if f.Column != "" {
				// Simple column reference — use table qualifier if present.
				idx, err := resolveColumnOffset(&ColumnRefExpr{Name: f.Column, Table: f.Table}, tables)
				name := f.Column
				if f.Alias != "" {
					name = f.Alias
				}
				if err == nil && idx >= 0 {
					fields = append(fields, projField{colIdx: idx, name: name})
				}
			} else if f.Expr != nil {
				name := f.Alias
				if name == "" {
					name = exprToString(f.Expr)
				}
				fields = append(fields, projField{colIdx: -1, expr: f.Expr, name: name})
			}
		}
	} else if len(s.SelectExprs) > 0 {
		for _, expr := range s.SelectExprs {
			if col, ok := expr.(*ColumnRefExpr); ok {
				idx, err := resolveColumnOffset(col, tables)
				if err == nil && idx >= 0 {
					fields = append(fields, projField{colIdx: idx, name: col.Name})
				}
			} else {
				name := exprToString(expr)
				fields = append(fields, projField{colIdx: -1, expr: expr, name: name})
			}
		}
	}

	if len(fields) == 0 {
		// Fallback: return all non-hidden columns.
		colNames := make([]string, 0)
		for _, entry := range tables {
			for _, col := range entry.td.Columns {
				if col.Hidden {
					continue
				}
				colNames = append(colNames, entry.alias+"."+col.Name)
			}
		}
		return rows, colNames
	}

	// Apply projection.
	colNames := make([]string, len(fields))
	for i, f := range fields {
		colNames[i] = f.name
	}

	projected := make([][]any, len(rows))
	for i, row := range rows {
		out := make([]any, len(fields))
		for j, f := range fields {
			if f.colIdx >= 0 {
				if f.colIdx < len(row) {
					out[j] = row[f.colIdx]
				}
			} else if f.expr != nil {
				out[j] = e.evalExprMultiJoin(f.expr, tables, row)
			}
		}
		projected[i] = out
	}
	return projected, colNames
}

// ─── Join cost model ───────────────────────────────────────────────────

func estimateHashJoinCost(buildRows, probeRows int64) float64 {
	buildScan := math.Max(1, float64(buildRows)/float64(rowsPerPage))*costSeqIO + float64(buildRows)*costCPURow
	probeScan := math.Max(1, float64(probeRows)/float64(rowsPerPage))*costSeqIO + float64(probeRows)*costCPURow
	hashBuild := float64(buildRows) * 0.011
	hashProbe := float64(probeRows) * 0.01
	return buildScan + probeScan + hashBuild + hashProbe
}

func estimateNestedLoopCost(leftRows, rightRows int64) float64 {
	outerScan := math.Max(1, float64(leftRows)/float64(rowsPerPage)) * costSeqIO
	innerScan := float64(leftRows) * math.Max(1, float64(rightRows)/float64(rowsPerPage)) * costSeqIO
	cpuCompare := float64(leftRows) * float64(rightRows) * costCPURow
	return outerScan + innerScan + cpuCompare
}

func estimateJoinCardinality(leftRows, rightRows int64, keys []joinEquiKey, leftTd, rightTd *catalog.TableDef) int64 {
	if len(keys) == 0 {
		return leftRows * rightRows
	}
	card := float64(leftRows) * float64(rightRows)
	for _, k := range keys {
		leftNDV := ndvForColumn(leftTd, k.leftName, leftRows)
		rightNDV := ndvForColumn(rightTd, k.rightName, rightRows)
		maxNDV := math.Max(float64(leftNDV), float64(rightNDV))
		if maxNDV > 0 {
			card /= maxNDV
		}
	}
	if card < 1 {
		if leftRows > 0 && rightRows > 0 {
			return 1
		}
		return 0
	}
	return int64(card)
}

func ndvForColumn(td *catalog.TableDef, colName string, rows int64) int64 {
	if td.Stats != nil {
		cs := colStatsByName(td.Stats, colName)
		if cs != nil && cs.NDV > 0 {
			return cs.NDV
		}
	}
	// Default: max(100, rows/10)
	def := rows / 10
	if def < 100 {
		def = 100
	}
	return def
}

// ─── planJoin — join optimizer ─────────────────────────────────────────

func (e *Executor) planJoin(ref *JoinTableRef, s *SelectStmt) *joinPlan {
	leftTd, _ := e.getTableDef(ref.Left)
	rightTd, _ := e.getTableDef(ref.Right)
	leftAlias := e.getTableAlias(ref.Left)
	rightAlias := e.getTableAlias(ref.Right)

	// 1. Extract equi-join keys + residual ON.
	keys, residualOn := extractEquiJoinKeys(ref.On, leftTd, rightTd, leftAlias, rightAlias)

	// 2. Split WHERE.
	leftWhere, rightWhere, remainWhere := splitWhereByTable(s.Where, leftTd, rightTd, leftAlias, rightAlias)

	// 3. Estimate row counts with pushdown.
	leftRows := tableRowCount(leftTd)
	rightRows := tableRowCount(rightTd)
	leftEstRows := estimateWHERECardinality(leftTd, leftWhere, leftRows)
	rightEstRows := estimateWHERECardinality(rightTd, rightWhere, rightRows)

	plan := &joinPlan{
		residualOn:  residualOn,
		leftWhere:   leftWhere,
		rightWhere:  rightWhere,
		remainWhere: remainWhere,
	}

	// 4. Determine build/probe sides based on join type.
	switch ref.Type {
	case JoinTypeLeft:
		// LEFT JOIN: build=right (inner), probe=left (outer)
		plan.buildSide = ref.Right
		plan.probeSide = ref.Left
		plan.buildTd = rightTd
		plan.probeTd = leftTd
		plan.buildAlias = rightAlias
		plan.probeAlias = leftAlias
		plan.estBuildRows = rightEstRows
		plan.estProbeRows = leftEstRows
		plan.swapped = false
	case JoinTypeRight:
		// RIGHT JOIN: build=left (inner), probe=right (outer)
		plan.buildSide = ref.Left
		plan.probeSide = ref.Right
		plan.buildTd = leftTd
		plan.probeTd = rightTd
		plan.buildAlias = leftAlias
		plan.probeAlias = rightAlias
		plan.estBuildRows = leftEstRows
		plan.estProbeRows = rightEstRows
		plan.swapped = false
	default:
		// INNER / CROSS: pick smaller table as build side.
		if leftEstRows <= rightEstRows {
			plan.buildSide = ref.Left
			plan.probeSide = ref.Right
			plan.buildTd = leftTd
			plan.probeTd = rightTd
			plan.buildAlias = leftAlias
			plan.probeAlias = rightAlias
			plan.estBuildRows = leftEstRows
			plan.estProbeRows = rightEstRows
			plan.swapped = false
		} else {
			plan.buildSide = ref.Right
			plan.probeSide = ref.Left
			plan.buildTd = rightTd
			plan.probeTd = leftTd
			plan.buildAlias = rightAlias
			plan.probeAlias = leftAlias
			plan.estBuildRows = rightEstRows
			plan.estProbeRows = leftEstRows
			plan.swapped = true
		}
	}

	// 5. Choose method: hash join if equi keys exist, else nested loop.
	if len(keys) > 0 {
		plan.method = "hash_join"
		// Map equi keys to build/probe column indices.
		remappedKeys := make([]joinEquiKey, 0, len(keys))
		for _, k := range keys {
			if plan.swapped {
				plan.probeKeyIdx = append(plan.probeKeyIdx, k.leftColIdx)
				remappedKeys = append(remappedKeys, joinEquiKey{
					leftColIdx:  k.rightColIdx, // build col
					rightColIdx: k.leftColIdx,  // probe col
					leftName:    k.rightName,
					rightName:   k.leftName,
				})
			} else {
				remappedKeys = append(remappedKeys, k)
				plan.probeKeyIdx = append(plan.probeKeyIdx, k.rightColIdx)
			}
		}
		plan.equiKeys = remappedKeys
		plan.estCost = estimateHashJoinCost(plan.estBuildRows, plan.estProbeRows)
	} else {
		plan.method = "nested_loop"
		plan.estCost = estimateNestedLoopCost(leftEstRows, rightEstRows)
	}

	// 6. Estimate output cardinality.
	plan.estRows = estimateJoinCardinality(leftEstRows, rightEstRows, keys, leftTd, rightTd)

	return plan
}

func tableRowCount(td *catalog.TableDef) int64 {
	if td == nil || td.Stats == nil {
		return 1000
	}
	return td.Stats.RowCount
}

// ─── collectRowsWithWhere — WHERE pushdown ─────────────────────────────

func (e *Executor) collectRowsWithWhere(t *txn.Txn, ref TableRef, td *catalog.TableDef, where Expr) ([][]any, error) {
	if where == nil {
		return e.collectRows(t, ref)
	}
	// Only apply WHERE filtering for simple table refs.
	if _, ok := ref.(*SimpleTableRef); !ok {
		return e.collectRows(t, ref)
	}
	storedTd, err := e.cat.GetTable(e.dbName, td.Name)
	if err != nil {
		return nil, err
	}
	treeKey := storedTd.DataFile()
	pkCols := storedTd.PrimaryKeyColumns()
	var rows [][]any
	t.Scan(treeKey, pkCols, []byte{0x00}, []byte{0xFF}, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, storedTd.Columns)
		if e.evalWhere(storedTd, where, vals) {
			rows = append(rows, vals)
		}
		return true
	})
	return rows, nil
}

// ─── execHashJoin — hash join execution ────────────────────────────────

func (e *Executor) execHashJoin(t *txn.Txn, s *SelectStmt, ref *JoinTableRef, leftTd, rightTd *catalog.TableDef, plan *joinPlan) (any, error) {
	// Build phase: collect build side rows into hash table.
	buildRows, err := e.collectRowsWithWhere(t, plan.buildSide, plan.buildTd, buildWhereForSide(plan, ref.Type))
	if err != nil {
		return nil, err
	}

	// Probe phase: collect probe side rows.
	var probeWhere Expr
	if ref.Type == JoinTypeLeft {
		probeWhere = plan.leftWhere
	} else if ref.Type == JoinTypeRight {
		probeWhere = plan.rightWhere
	} else {
		// INNER: probe side might be left or right depending on swapped.
		if plan.swapped {
			probeWhere = plan.leftWhere
		} else {
			probeWhere = plan.rightWhere
		}
	}
	probeRows, err := e.collectRowsWithWhere(t, plan.probeSide, plan.probeTd, probeWhere)
	if err != nil {
		return nil, err
	}

	// Build hash table: key → list of rows.
	hashTable := make(map[string][][]any)
	for _, row := range buildRows {
		key, skip := hashJoinKey(row, plan.equiKeys, true)
		if skip {
			continue
		}
		hashTable[key] = append(hashTable[key], row)
	}

	// Determine actual left/right td and aliases.
	leftAlias := e.getTableAlias(ref.Left)
	rightAlias := e.getTableAlias(ref.Right)

	var resultRows [][]any

	for pi, probeRow := range probeRows {
		key, skip := hashJoinKey(probeRow, plan.equiKeys, false)
		if skip {
			// NULL in join key: for LEFT/RIGHT join, output with NULLs on other side.
			if ref.Type == JoinTypeLeft || ref.Type == JoinTypeRight {
				var joined []any
				if ref.Type == JoinTypeLeft {
					// probe = left, build = right → (left, NULL_right)
					joined = append(append([]any{}, probeRow...), make([]any, len(plan.buildTd.Columns))...)
				} else {
					// probe = right, build = left → (NULL_left, right)
					joined = append(append([]any{}, make([]any, len(plan.buildTd.Columns))...), probeRow...)
				}
				if plan.remainWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, plan.remainWhere) {
					resultRows = append(resultRows, joined)
				}
			}
			continue
		}

		matches := hashTable[key]
		anyMatched := false
		for _, buildRow := range matches {
			// Reassemble left+right order.
			var joined []any
			if plan.swapped {
				// build=right, probe=left → probe|build = left|right
				joined = append(append([]any{}, probeRow...), buildRow...)
			} else if ref.Type == JoinTypeLeft {
				// probe=left, build=right → probe|build = left|right
				joined = append(append([]any{}, probeRow...), buildRow...)
			} else if ref.Type == JoinTypeRight {
				// probe=right, build=left → build|probe = left|right
				joined = append(append([]any{}, buildRow...), probeRow...)
			} else {
				// INNER, not swapped: build=left, probe=right → build|probe = left|right
				joined = append(append([]any{}, buildRow...), probeRow...)
			}

			// Check residual ON condition.
			if plan.residualOn != nil {
				if !e.evalJoinCondition(leftTd, rightTd, leftAlias, rightAlias, joined[:len(leftTd.Columns)], joined[len(leftTd.Columns):], plan.residualOn) {
					continue
				}
			}

			anyMatched = true

			// Check remaining WHERE.
			if plan.remainWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, plan.remainWhere) {
				resultRows = append(resultRows, joined)
			}
		}

		// Handle unmatched probe rows for LEFT/RIGHT join.
		if !anyMatched {
			var joined []any
			if ref.Type == JoinTypeLeft {
				// probe=left → (left, NULL_right)
				joined = append(append([]any{}, probeRow...), make([]any, len(plan.buildTd.Columns))...)
			} else if ref.Type == JoinTypeRight {
				// probe=right → (NULL_left, right)
				joined = append(append([]any{}, make([]any, len(plan.buildTd.Columns))...), probeRow...)
			}
			if joined != nil {
				if plan.remainWhere == nil || e.evalJoinWhere(leftTd, rightTd, leftAlias, rightAlias, joined, plan.remainWhere) {
					resultRows = append(resultRows, joined)
				}
			}
		}
		_ = pi
	}

	entries := twoTableJoinEntries(leftTd, rightTd, leftAlias, rightAlias)
	projectedRows, colNames := e.projectMultiJoinResult(s, entries, resultRows)
	return &SelectResult{Columns: colNames, Rows: projectedRows, TableAlias: leftAlias + " join " + rightAlias}, nil
}

// buildWhereForSide returns the pushdown WHERE for the build side.
func buildWhereForSide(plan *joinPlan, joinType JoinType) Expr {
	if joinType == JoinTypeLeft {
		return plan.rightWhere // build = right
	}
	if joinType == JoinTypeRight {
		return plan.leftWhere // build = left
	}
	// INNER: swapped means build=right, else build=left
	if plan.swapped {
		return plan.rightWhere
	}
	return plan.leftWhere
}

// hashJoinKey serializes a row's join key columns into a string for the hash table.
// Returns skip=true if any key column is NULL.
func hashJoinKey(row []any, keys []joinEquiKey, isBuild bool) (string, bool) {
	var parts []string
	for _, k := range keys {
		var idx int
		if isBuild {
			idx = k.leftColIdx // build key index
		} else {
			idx = k.rightColIdx // probe key index (stored as rightColIdx)
		}
		if idx >= len(row) {
			return "", true
		}
		val := row[idx]
		if val == nil {
			return "", true // NULL keys don't match
		}
		parts = append(parts, fmt.Sprintf("%v", val))
	}
	return strings.Join(parts, "\x00"), false
}
