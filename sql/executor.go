package sql

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"lns.com/minidb/catalog"
	"lns.com/minidb/metrics"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
)

// Result types.
type (
	SelectResult struct {
		Columns    []string
		Rows       [][]any
		TableAlias string
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
func (e *Executor) Execute(sql string) (any, error) {
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
	case *ExplainStmt:
		return e.execExplain(s)
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
	if len(pkCols) == 0 {
		pkCols = []int{0}
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
		pkVals := make([]any, len(td.PKCols))
		for i, colIdx := range td.PKCols {
			pkVals[i] = vals[colIdx]
		}
		idxVals := make([]any, len(idxDef.Columns))
		for j, colName := range idxDef.Columns {
			idxVals[j] = vals[td.ColumnIndex(colName)]
		}
		idxKey := storage.EncodeIndexKey(idxColDefs, idxVals, pkCols, pkVals...)
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

func (e *Executor) execDropTable(s *DropTableStmt) (any, error) {
	if e.dbName == "" {
		return nil, fmt.Errorf("no database selected")
	}
	return &OKResult{}, e.cat.DropTable(e.dbName, s.Table)
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
			int64(0),   // Cardinality (unknown)
			nil,         // Sub_part
			nil,         // Packed
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

func (e *Executor) execDML(fn func(*txn.Txn) (any, error)) (any, error) {
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

func (e *Executor) execDMLRead(fn func(*txn.Txn) (any, error)) (any, error) {
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
			if len(rowVals) != len(td.Columns) {
				return nil, fmt.Errorf("column count mismatch: got %d, expected %d", len(rowVals), len(td.Columns))
			}
			fullVals = rowVals
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
			idxKey := storage.EncodeIndexKey(idxColDefs, idxVals, pkCols, pkVals...)
			t.Insert(idxTreeKey, idxKey, nil)
		}
	}

	return &OKResult{AffectedRows: len(s.Values), InsertID: lastID}, nil
}

func (e *Executor) execSelect(t *txn.Txn, s *SelectStmt) (any, error) {
	if s.TableRef == nil {
		return nil, fmt.Errorf("no table specified")
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

func (e *Executor) execSelectSimple(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef) (any, error) {
	totalStart := time.Now()
	defer func() {
		metrics.SelectSimpleDuration.Observe(time.Since(totalStart).Seconds())
	}()

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

	if len(s.SelectExprs) > 0 && len(s.Columns) == 0 && !s.SelectAll {
		return e.execSelectAggregate(t, s, ref, td)
	}

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
		colNames = make([]string, len(s.Columns))
		for i, name := range s.Columns {
			colNames[i] = name
			idx := td.ColumnIndex(name)
			if idx < 0 {
				return nil, fmt.Errorf("unknown column %q", name)
			}
			colIndices[i] = idx
		}
	}
	metrics.SelectResolveDuration.Observe(time.Since(t0).Seconds())

	// Stage 2: Optimization path selection.
	t1 := time.Now()
	var rows [][]any
	if s.Where != nil {
		if inRows, ok := e.tryINOnPK(t, td, treeKey, s.Where, colIndices); ok {
			rows = inRows
		}
	}

	// Optimization: use secondary index if WHERE matches an index prefix.
	if rows == nil && s.Where != nil && len(td.Indexes) > 0 {
		if idxRows, ok := e.tryIndexScan(t, td, s, colIndices, colNames); ok {
			rows = idxRows
		}
	}
	metrics.SelectOptPathDuration.Observe(time.Since(t1).Seconds())

	// Stage 3: Scan loop (may include t.Scan internally for opt paths).
	if rows == nil {
		t2 := time.Now()
		var start, end []byte
		if s.Where != nil {
			start, end = e.extractPKRange(td, s.Where)
		}
		if start == nil {
			start = []byte{0x00}
			end = []byte{0xFF}
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

		pkCols := td.PrimaryKeyColumns()
		t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
			vals, _ := storage.DecodeRow(rowData, td.Columns)
			if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
				return true
			}
			row := make([]any, len(colIndices))
			for i, ci := range colIndices {
				row[i] = vals[ci]
			}
			rows = append(rows, row)
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
		e.sortRows(rows, colNames, s.OrderBy)
	}
	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}
	metrics.SelectPostProcessDuration.Observe(time.Since(t3).Seconds())

	return &SelectResult{Columns: colNames, Rows: rows, TableAlias: tableName}, nil
}

// tryINOnPK checks if the WHERE clause is a simple col IN (v1, v2, ...) on a
// single-column primary key. If so, it performs point lookups for each value
// and returns (rows, true). Otherwise returns (nil, false).
func (e *Executor) tryINOnPK(t *txn.Txn, td *catalog.TableDef, treeKey string, where Expr, colIndices []int) ([][]any, bool) {
	inExpr, ok := where.(*InExpr)
	if !ok || inExpr.Not || len(inExpr.Values) == 0 {
		return nil, false
	}

	// Check that the IN target is a column reference.
	col, ok := inExpr.Expr.(*ColumnRefExpr)
	if !ok {
		return nil, false
	}

	// Must be a single-column PK and the column must be that PK.
	if len(td.PKCols) != 1 {
		return nil, false
	}
	pkColIdx := td.PKCols[0]
	if td.Columns[pkColIdx].Name != col.Name {
		return nil, false
	}

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

	// Fast path: SELECT COUNT(1) / COUNT(*) without WHERE.
	if s.Where == nil && len(s.SelectExprs) == 1 {
		var countFunc bool
		if f, ok := s.SelectExprs[0].(*FuncCallExpr); ok && strings.ToUpper(f.Name) == "COUNT" {
			countFunc = true
		} else if f, ok := s.SelectExprs[0].(*AggregateFuncExpr); ok && strings.ToUpper(f.Name) == "COUNT" {
			countFunc = true
		}
		if countFunc {
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
		count   int64
		sum     float64
		minVal  any
		maxVal  any
		hasData bool
	}

	agg := &aggState{}

	e.engine.ScanAll(treeKey, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhere(td, s.Where, vals) {
			return true
		}
		agg.count++
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
					agg.count = agg.count
				case "SUM":
					for _, arg := range args {
						v := e.evalExpr(td, arg, vals)
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
						v := e.evalExpr(td, arg, vals)
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
			row[i] = agg.sum
			colNames[i] = "sum"
		case "AVG":
			if agg.count > 0 {
				row[i] = agg.sum / float64(agg.count)
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

func (e *Executor) execSelectJoin(t *txn.Txn, s *SelectStmt, ref *JoinTableRef) (any, error) {
	leftRows, err := e.collectRows(t, ref.Left)
	if err != nil {
		return nil, err
	}
	rightRows, err := e.collectRows(t, ref.Right)
	if err != nil {
		return nil, err
	}

	leftTd, err := e.getTableDef(ref.Left)
	if err != nil {
		return nil, err
	}
	rightTd, err := e.getTableDef(ref.Right)
	if err != nil {
		return nil, err
	}

	var rows [][]any
	leftAlias := e.getTableAlias(ref.Left)
	rightAlias := e.getTableAlias(ref.Right)

	isLeftJoin := ref.Type == JoinTypeLeft
	isRightJoin := ref.Type == JoinTypeRight
	leftNullRow := make([]any, len(leftTd.Columns))

	if isRightJoin {
		rightMatched := make(map[int]bool)
		for ri, rightRow := range rightRows {
			matched := false
			for li, leftRow := range leftRows {
				if e.evalJoinCondition(leftTd, rightTd, leftRow, rightRow, ref.On) {
					matched = true
					rightMatched[ri] = true
					joined := append(append([]any{}, leftRow...), rightRow...)
					if s.Where == nil || e.evalJoinWhere(leftTd, rightTd, joined, s.Where) {
						rows = append(rows, joined)
					}
					_ = li
				}
			}
			if !matched {
				joined := append(append([]any{}, leftNullRow...), rightRow...)
				if s.Where == nil || e.evalJoinWhere(leftTd, rightTd, joined, s.Where) {
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
				if e.evalJoinCondition(leftTd, rightTd, leftRow, rightRow, ref.On) {
					matched = true
					joined := append(append([]any{}, leftRow...), rightRow...)
					if s.Where == nil || e.evalJoinWhere(leftTd, rightTd, joined, s.Where) {
						rows = append(rows, joined)
					}
				}
			}
			if !matched && isLeftJoin {
				joined := append(append([]any{}, leftRow...), rightNullRow...)
				if s.Where == nil || e.evalJoinWhere(leftTd, rightTd, joined, s.Where) {
					rows = append(rows, joined)
				}
			}
		}
	}

	colNames := make([]string, 0, len(leftTd.Columns)+len(rightTd.Columns))
	for _, col := range leftTd.Columns {
		colNames = append(colNames, leftAlias+"."+col.Name)
	}
	for _, col := range rightTd.Columns {
		colNames = append(colNames, rightAlias+"."+col.Name)
	}

	return &SelectResult{Columns: colNames, Rows: rows, TableAlias: leftAlias + " join " + rightAlias}, nil
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
				if e.evalJoinCondition(leftTd, rightTd, lr, rr, r.On) {
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

func (e *Executor) evalJoinCondition(leftTd, rightTd *catalog.TableDef, leftRow, rightRow []any, on Expr) bool {
	if on == nil {
		return true
	}
	leftVals := leftRow
	rightVals := rightRow
	return e.evalBinaryExprForJoin(on, leftTd, leftVals, rightTd, rightVals)
}

func (e *Executor) evalBinaryExprForJoin(expr Expr, leftTd *catalog.TableDef, leftVals []any, rightTd *catalog.TableDef, rightVals []any) bool {
	if bin, ok := expr.(*BinaryExpr); ok {
		if bin.Op == "AND" {
			return e.evalBinaryExprForJoin(bin.Left, leftTd, leftVals, rightTd, rightVals) &&
				e.evalBinaryExprForJoin(bin.Right, leftTd, leftVals, rightTd, rightVals)
		}
		lv := e.evalColumnRef(bin.Left, leftTd, leftVals, rightTd, rightVals)
		rv := e.evalColumnRef(bin.Right, leftTd, leftVals, rightTd, rightVals)
		cmp := compareValues(lv, rv)
		switch bin.Op {
		case "=":
			return cmp == 0
		case "!=":
			return cmp != 0
		case "<":
			return cmp < 0
		case "<=":
			return cmp <= 0
		case ">":
			return cmp > 0
		case ">=":
			return cmp >= 0
		}
	}
	return true
}

func (e *Executor) evalColumnRef(expr Expr, leftTd *catalog.TableDef, leftVals []any, rightTd *catalog.TableDef, rightVals []any) any {
	switch ex := expr.(type) {
	case *ColumnRefExpr:
		if ex.Table != "" && ex.Table != leftTd.Name {
			idx := rightTd.ColumnIndex(ex.Name)
			if idx >= 0 {
				return rightVals[idx]
			}
			return nil
		}
		idx := leftTd.ColumnIndex(ex.Name)
		if idx >= 0 {
			return leftVals[idx]
		}
		idx = rightTd.ColumnIndex(ex.Name)
		if idx >= 0 {
			return rightVals[idx]
		}
	case *LiteralExpr:
		return ex.Value
	}
	return nil
}

func (e *Executor) evalJoinWhere(leftTd, rightTd *catalog.TableDef, joinedRow []any, where Expr) bool {
	leftLen := len(leftTd.Columns)
	leftVals := joinedRow[:leftLen]
	rightVals := joinedRow[leftLen:]
	return e.evalBinaryExprForJoin(where, leftTd, leftVals, rightTd, rightVals)
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
			pkVals := make([]any, len(td.PKCols))
			for i, colIdx := range td.PKCols {
				pkVals[i] = vals[colIdx]
			}
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

				// Delete old index entry.
				oldIdxVals := make([]any, len(idx.Columns))
				for k, colName := range idx.Columns {
					oldIdxVals[k] = vals[td.ColumnIndex(colName)]
				}
				oldKey := storage.EncodeIndexKey(idxColDefs, oldIdxVals, pkCols, pkVals...)
				t.Delete(idxTreeKey, pkCols, oldKey)

				// Insert new index entry.
				newIdxVals := make([]any, len(idx.Columns))
				for k, colName := range idx.Columns {
					newIdxVals[k] = newVals[td.ColumnIndex(colName)]
				}
				newKey := storage.EncodeIndexKey(idxColDefs, newIdxVals, pkCols, pkVals...)
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
			pkVals := make([]any, len(td.PKCols))
			for i, colIdx := range td.PKCols {
				pkVals[i] = vals[colIdx]
			}
			for j := range td.Indexes {
				idx := &td.Indexes[j]
				idxTreeKey := td.IndexFile(idx)
				idxColDefs := idxColumnDefs(td, idx)
				idxVals := make([]any, len(idx.Columns))
				for k, colName := range idx.Columns {
					idxVals[k] = vals[td.ColumnIndex(colName)]
				}
				idxKey := storage.EncodeIndexKey(idxColDefs, idxVals, pkCols, pkVals...)
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
	tableName := e.getTableAlias(s.TableRef)
	if tableName == "" {
		tableName = "unknown"
	}
	selectType := "SIMPLE"
	if _, ok := s.TableRef.(*JoinTableRef); ok {
		selectType = "JOIN"
	}

	accessType, possibleKeys, usedKey, extra := e.explainIndexInfoForSelect(tableName, s)

	r.Rows = append(r.Rows, []any{
		1,
		selectType,
		tableName,
		accessType,
		possibleKeys,
		usedKey,
		nil,
		nil,
		"estimate",
		extra,
	})
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
	default:
		return nil
	}
}

func (e *Executor) execSubquery(query *SelectStmt) any {
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

func (e *Executor) evalInExpr(td *catalog.TableDef, expr *InExpr, vals []any) bool {
	val := e.evalExpr(td, expr.Expr, vals)
	for _, v := range expr.Values {
		if sq, ok := v.(*SubqueryExpr); ok {
			result := e.execSubquery(sq.Query)
			if rs, ok := result.(*SelectResult); ok {
				for _, row := range rs.Rows {
					if len(row) > 0 && compareValues(val, row[0]) == 0 {
						return !expr.Not
					}
				}
				return expr.Not
			}
		} else {
			v := e.evalExpr(td, v, vals)
			if compareValues(val, v) == 0 {
				return !expr.Not
			}
		}
	}
	return expr.Not
}

func (e *Executor) evalExistsExpr(td *catalog.TableDef, expr *ExistsExpr, vals []any) bool {
	rs, err := e.execSubqueryWithOuter(expr.Query, td, vals)
	if err != nil || rs == nil {
		return expr.Not
	}
	exists := len(rs.Rows) > 0
	if expr.Not {
		return !exists
	}
	return exists
}

func (e *Executor) execSubqueryWithOuter(query *SelectStmt, outerTd *catalog.TableDef, outerVals []any) (*SelectResult, error) {
	switch ref := query.TableRef.(type) {
	case *SimpleTableRef:
		txn := e.mgr.Begin()
		defer txn.Rollback()
		return e.execSelectSimpleWithOuter(txn, query, ref, outerTd, outerVals)
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

func (e *Executor) execSelectSimpleWithOuter(t *txn.Txn, s *SelectStmt, ref *SimpleTableRef, outerTd *catalog.TableDef, outerVals []any) (*SelectResult, error) {
	td, err := e.cat.GetTable(e.dbName, ref.Table)
	if err != nil {
		return nil, err
	}

	treeKey := td.DataFile()
	var colIndices []int
	var colNames []string
	if s.SelectAll {
		colNames = make([]string, len(td.Columns))
		colIndices = make([]int, len(td.Columns))
		for i, col := range td.Columns {
			colNames[i] = col.Name
			colIndices[i] = i
		}
	} else if len(s.Columns) > 0 {
		colIndices = make([]int, len(s.Columns))
		colNames = make([]string, len(s.Columns))
		for i, name := range s.Columns {
			colNames[i] = name
			idx := td.ColumnIndex(name)
			if idx < 0 {
				idx = 0
			}
			colIndices[i] = idx
		}
	} else {
		colIndices = []int{0}
		colNames = []string{"id"}
	}

	start, end := []byte{0x00}, []byte{0xFF}

	var rows [][]any
	pkCols := td.PrimaryKeyColumns()

	t.Scan(treeKey, pkCols, start, end, func(pk, rowData []byte) bool {
		vals, _ := storage.DecodeRow(rowData, td.Columns)
		if s.Where != nil && !e.evalWhereWithOuter(td, vals, s.Where, outerTd, outerVals) {
			return true
		}
		if len(colIndices) > 0 {
			row := make([]any, len(colIndices))
			for i, ci := range colIndices {
				if ci < len(vals) {
					row[i] = vals[ci]
				}
			}
			rows = append(rows, row)
		} else {
			rows = append(rows, []any{1})
		}
		return true
	})

	if len(s.OrderBy) > 0 {
		e.sortRows(rows, colNames, s.OrderBy)
	}
	if s.Limit != nil && *s.Limit < len(rows) {
		rows = rows[:*s.Limit]
	}

	return &SelectResult{Columns: colNames, Rows: rows}, nil
}

func (e *Executor) evalWhereWithOuter(td *catalog.TableDef, vals []any, where Expr, outerTd *catalog.TableDef, outerVals []any) bool {
	result := e.evalExprWithOuter(td, vals, where, outerTd, outerVals)
	if b, ok := result.(bool); ok {
		return b
	}
	if result == nil {
		return false
	}
	return true
}

func (e *Executor) evalExprWithOuter(td *catalog.TableDef, vals []any, expr Expr, outerTd *catalog.TableDef, outerVals []any) any {
	switch ex := expr.(type) {
	case *LiteralExpr:
		return ex.Value
	case *ColumnRefExpr:
		colName := ex.Name
		tableName := ex.Table
		if tableName != "" && tableName != td.Name {
			if outerTd != nil && tableName == outerTd.Name {
				idx := outerTd.ColumnIndex(colName)
				if idx >= 0 {
					return outerVals[idx]
				}
			}
			return nil
		}
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
		left := e.evalExprWithOuter(td, vals, ex.Left, outerTd, outerVals)
		right := e.evalExprWithOuter(td, vals, ex.Right, outerTd, outerVals)
		return e.evalBinaryOp(ex.Op, left, right)
	case *UnaryExpr:
		operand := e.evalExprWithOuter(td, vals, ex.Operand, outerTd, outerVals)
		return e.evalUnaryOp(ex.Op, operand)
	case *NullExpr:
		return nil
	case *IsNullExpr:
		v := e.evalExprWithOuter(td, vals, ex.Expr, outerTd, outerVals)
		if ex.Not {
			return v != nil
		}
		return v == nil
	case *SubqueryExpr:
		result, err := e.execSubqueryWithOuter(ex.Query, outerTd, outerVals)
		if err != nil || result == nil {
			return nil
		}
		if len(result.Rows) == 1 && len(result.Rows[0]) == 1 {
			return result.Rows[0][0]
		}
		return result
	case *InExpr:
		return e.evalInExprWithOuter(td, vals, ex, outerTd, outerVals)
	case *ExistsExpr:
		return e.evalExistsExprWithOuter(td, vals, ex, outerTd, outerVals)
	case *LikeExpr:
		return e.evalLikeExprWithOuter(td, vals, ex, outerTd, outerVals)
	default:
		return nil
	}
}

func (e *Executor) evalInExprWithOuter(td *catalog.TableDef, vals []any, expr *InExpr, outerTd *catalog.TableDef, outerVals []any) bool {
	val := e.evalExprWithOuter(td, vals, expr.Expr, outerTd, outerVals)
	for _, v := range expr.Values {
		if sq, ok := v.(*SubqueryExpr); ok {
			result, err := e.execSubqueryWithOuter(sq.Query, outerTd, outerVals)
			if err != nil || result == nil {
				return expr.Not
			}
			for _, row := range result.Rows {
				if len(row) > 0 && compareValues(val, row[0]) == 0 {
					return !expr.Not
				}
			}
			return expr.Not
		} else {
			v := e.evalExprWithOuter(td, vals, v, outerTd, outerVals)
			if compareValues(val, v) == 0 {
				return !expr.Not
			}
		}
	}
	return expr.Not
}

func (e *Executor) evalExistsExprWithOuter(td *catalog.TableDef, vals []any, expr *ExistsExpr, outerTd *catalog.TableDef, outerVals []any) bool {
	rs, err := e.execSubqueryWithOuter(expr.Query, outerTd, outerVals)
	if err != nil {
		return expr.Not
	}
	exists := len(rs.Rows) > 0
	if expr.Not {
		return !exists
	}
	return exists
}

func (e *Executor) evalLikeExprWithOuter(td *catalog.TableDef, vals []any, expr *LikeExpr, outerTd *catalog.TableDef, outerVals []any) bool {
	val := e.evalExprWithOuter(td, vals, expr.Expr, outerTd, outerVals)
	pattern := e.evalExprWithOuter(td, vals, expr.Pattern, outerTd, outerVals)
	result := e.evalLike(val, pattern)
	if expr.Not {
		return !result
	}
	return result
}

func (e *Executor) evalBinaryOp(op string, left, right any) any {
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
// Supports equality conditions on consecutive PK columns, plus one
// range condition (>=, >, <=, <, BETWEEN) on the next PK column.
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
	lowVal, highVal any
	hasLow, hasHigh  bool
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
			if e.lessThan(rows[j], rows[j-1], orderBy, colIdx) {
				rows[j], rows[j-1] = rows[j-1], rows[j]
			} else {
				break
			}
		}
	}
}

func (e *Executor) lessThan(a, b []any, orderBy []OrderByClause, colIdx map[string]int) bool {
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

func toBool(v any) bool {
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

func arithOp(a, b any, intFn func(int64, int64) int64, floatFn func(float64, float64) float64) any {
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
