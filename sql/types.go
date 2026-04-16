package sql

import (
	"fmt"
	"strconv"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/opcode"
	"github.com/pingcap/tidb/pkg/parser/types"
)

// Stmt types.
type (
	CreateDatabaseStmt struct{ Name string }
	DropDatabaseStmt   struct{ Name string }
	UseStmt            struct{ DBName string }
	CreateTableStmt    struct {
		Table   string
		Columns []ColumnDef
	}
	DropTableStmt     struct{ Table string }
	ShowTablesStmt    struct{}
	ShowDatabasesStmt struct{}
	DescTableStmt     struct{ Table string }

	InsertStmt struct {
		Table   string
		Columns []string
		Values  [][]any
	}
	SelectStmt struct {
		Table     string
		Columns   []string
		SelectAll bool
		Where     Expr
		OrderBy   []OrderByClause
		Limit     *int
		ForUpdate bool
	}
	UpdateStmt struct {
		Table      string
		SetClauses []SetClause
		Where      Expr
	}
	DeleteStmt struct {
		Table string
		Where Expr
	}

	BeginStmt    struct{}
	CommitStmt   struct{}
	RollbackStmt struct{}
)

type ColumnDef struct {
	Name      string
	Type      string
	Length    int
	Precision int
	Scale     int
	Nullable  bool
	AutoInc   bool
	Primary   bool
}

type SetClause struct {
	Column string
	Value  Expr
}

type OrderByClause struct {
	Column string
	Desc   bool
}

// Expr types.
type (
	Expr interface{ exprNode() }

	LiteralExpr   struct{ Value any }
	ColumnRefExpr struct{ Name string }
	BinaryExpr    struct {
		Op          string
		Left, Right Expr
	}
	UnaryExpr struct {
		Op      string
		Operand Expr
	}
	FuncCallExpr struct {
		Name string
		Args []Expr
	}
	ParamExpr   struct{}
	NullExpr    struct{}
	BetweenExpr struct{ Expr, Low, High Expr }
	InExpr      struct {
		Expr   Expr
		Values []Expr
		Not    bool
	}
	IsNullExpr struct {
		Expr Expr
		Not  bool
	}
)

func (LiteralExpr) exprNode()   {}
func (ColumnRefExpr) exprNode() {}
func (BinaryExpr) exprNode()    {}
func (UnaryExpr) exprNode()     {}
func (FuncCallExpr) exprNode()  {}
func (ParamExpr) exprNode()     {}
func (NullExpr) exprNode()      {}
func (BetweenExpr) exprNode()   {}
func (InExpr) exprNode()        {}
func (IsNullExpr) exprNode()    {}

type Stmt any

// Parser wraps the TiDB parser.
type Parser struct {
	p *parser.Parser
}

func NewParser() *Parser {
	return &Parser{p: parser.New()}
}

func (p *Parser) Parse(sql string) (Stmt, error) {
	stmts, _, err := p.p.Parse(sql, "", "")
	if err != nil {
		return nil, fmt.Errorf("parse error: %w", err)
	}
	if len(stmts) == 0 {
		return nil, fmt.Errorf("empty statement")
	}
	return convertStmt(stmts[0])
}

func convertStmt(node ast.StmtNode) (Stmt, error) {
	switch n := node.(type) {
	case *ast.CreateDatabaseStmt:
		return &CreateDatabaseStmt{Name: n.Name.O}, nil
	case *ast.DropDatabaseStmt:
		return &DropDatabaseStmt{Name: n.Name.O}, nil
	case *ast.UseStmt:
		return &UseStmt{DBName: n.DBName}, nil
	case *ast.ShowStmt:
		switch n.Tp {
		case ast.ShowTables:
			return &ShowTablesStmt{}, nil
		case ast.ShowDatabases:
			return &ShowDatabasesStmt{}, nil
		}
		return nil, fmt.Errorf("unsupported SHOW type: %v", n.Tp)
	case *ast.CreateTableStmt:
		return convertCreateTable(n)
	case *ast.DropTableStmt:
		if len(n.Tables) > 0 {
			return &DropTableStmt{Table: n.Tables[0].Name.O}, nil
		}
		return nil, fmt.Errorf("DROP TABLE: no table specified")
	case *ast.InsertStmt:
		return convertInsert(n)
	case *ast.SelectStmt:
		return convertSelect(n)
	case *ast.UpdateStmt:
		return convertUpdate(n)
	case *ast.DeleteStmt:
		return convertDelete(n)
	case *ast.BeginStmt:
		return &BeginStmt{}, nil
	case *ast.CommitStmt:
		return &CommitStmt{}, nil
	case *ast.RollbackStmt:
		return &RollbackStmt{}, nil
	case *ast.ExplainStmt:
		if show, ok := n.Stmt.(*ast.ShowStmt); ok {
			return &DescTableStmt{Table: show.Table.Name.O}, nil
		}
		return nil, fmt.Errorf("unsupported EXPLAIN statement")
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", node)
	}
}

func convertCreateTable(n *ast.CreateTableStmt) (*CreateTableStmt, error) {
	result := &CreateTableStmt{Table: n.Table.Name.O}
	for _, col := range n.Cols {
		cd := ColumnDef{Name: col.Name.Name.O}
		tp := col.Tp
		if tp != nil {
			cd.Type = getTypeName(tp)
			cd.Length = tp.GetFlen()
			cd.Precision = tp.GetFlen()
			cd.Scale = tp.GetDecimal()
			cd.Nullable = !mysql.HasNotNullFlag(tp.GetFlag())
			cd.AutoInc = mysql.HasAutoIncrementFlag(tp.GetFlag())
		}
		for _, opt := range col.Options {
			switch opt.Tp {
			case ast.ColumnOptionPrimaryKey:
				cd.Primary = true
			case ast.ColumnOptionAutoIncrement:
				cd.AutoInc = true
			case ast.ColumnOptionNotNull:
				cd.Nullable = false
			}
		}
		result.Columns = append(result.Columns, cd)
	}

	// Handle table-level constraints (e.g., PRIMARY KEY (col1, col2)).
	for _, constraint := range n.Constraints {
		if constraint.Tp == ast.ConstraintPrimaryKey {
			for _, col := range result.Columns {
				col.Primary = false // clear any column-level PK
			}
			for _, key := range constraint.Keys {
				colName := key.Column.Name.O
				for i := range result.Columns {
					if result.Columns[i].Name == colName {
						result.Columns[i].Primary = true
						break
					}
				}
			}
		}
	}

	return result, nil
}

func getTypeName(tp *types.FieldType) string {
	switch tp.GetType() {
	case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24, mysql.TypeLong:
		return "INT"
	case mysql.TypeLonglong:
		return "BIGINT"
	case mysql.TypeVarchar, mysql.TypeString, mysql.TypeVarString:
		return "VARCHAR"
	case mysql.TypeNewDecimal:
		return "DECIMAL"
	case mysql.TypeDouble, mysql.TypeFloat:
		return "DOUBLE"
	case mysql.TypeTimestamp, mysql.TypeDatetime:
		return "TIMESTAMP"
	default:
		return "UNKNOWN"
	}
}

func convertInsert(n *ast.InsertStmt) (*InsertStmt, error) {
	tableName := extractFromTableRefs(n.Table)
	result := &InsertStmt{Table: tableName}
	for _, col := range n.Columns {
		result.Columns = append(result.Columns, col.Name.O)
	}
	for _, row := range n.Lists {
		var vals []any
		for _, expr := range row {
			v, err := evalLiteral(expr)
			if err != nil {
				return nil, err
			}
			vals = append(vals, v)
		}
		result.Values = append(result.Values, vals)
	}
	return result, nil
}

func convertSelect(n *ast.SelectStmt) (*SelectStmt, error) {
	result := &SelectStmt{}
	if n.From != nil && n.From.TableRefs != nil {
		result.Table = extractTable(n.From.TableRefs)
	}
	if n.Fields != nil {
		for _, field := range n.Fields.Fields {
			if field.WildCard != nil {
				result.SelectAll = true
			} else {
				result.Columns = append(result.Columns, getColumnName(field.Expr))
			}
		}
	}
	if n.Where != nil {
		expr, err := convertExpr(n.Where)
		if err != nil {
			return nil, err
		}
		result.Where = expr
	}
	if n.OrderBy != nil {
		for _, item := range n.OrderBy.Items {
			result.OrderBy = append(result.OrderBy, OrderByClause{
				Column: getColumnName(item.Expr),
				Desc:   item.Desc,
			})
		}
	}
	if n.Limit != nil && n.Limit.Count != nil {
		if count, err := evalLiteralInt(n.Limit.Count); err == nil {
			result.Limit = &count
		}
	}
	result.ForUpdate = n.LockInfo != nil
	return result, nil
}

func convertUpdate(n *ast.UpdateStmt) (*UpdateStmt, error) {
	result := &UpdateStmt{}
	if n.TableRefs != nil && n.TableRefs.TableRefs != nil {
		result.Table = extractTable(n.TableRefs.TableRefs)
	}
	for _, item := range n.List {
		result.SetClauses = append(result.SetClauses, SetClause{
			Column: item.Column.Name.O,
			Value:  mustConvertExpr(item.Expr),
		})
	}
	if n.Where != nil {
		expr, err := convertExpr(n.Where)
		if err != nil {
			return nil, err
		}
		result.Where = expr
	}
	return result, nil
}

func convertDelete(n *ast.DeleteStmt) (*DeleteStmt, error) {
	result := &DeleteStmt{}
	if n.TableRefs != nil && n.TableRefs.TableRefs != nil {
		result.Table = extractTable(n.TableRefs.TableRefs)
	}
	if n.Where != nil {
		expr, err := convertExpr(n.Where)
		if err != nil {
			return nil, err
		}
		result.Where = expr
	}
	return result, nil
}

// --- Expression conversion ---

func convertExpr(node ast.ExprNode) (Expr, error) {
	switch n := node.(type) {
	case ast.ValueExpr:
		return &LiteralExpr{Value: n.GetValue()}, nil
	case ast.ParamMarkerExpr:
		return &ParamExpr{}, nil
	case *ast.ColumnNameExpr:
		return &ColumnRefExpr{Name: n.Name.Name.O}, nil
	case *ast.BinaryOperationExpr:
		left, err := convertExpr(n.L)
		if err != nil {
			return nil, err
		}
		right, err := convertExpr(n.R)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: opToString(n.Op), Left: left, Right: right}, nil
	case *ast.UnaryOperationExpr:
		operand, err := convertExpr(n.V)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: unaryOpToString(n.Op), Operand: operand}, nil
	case *ast.IsNullExpr:
		expr, err := convertExpr(n.Expr)
		if err != nil {
			return nil, err
		}
		return &IsNullExpr{Expr: expr, Not: n.Not}, nil
	case *ast.BetweenExpr:
		expr, err := convertExpr(n.Expr)
		if err != nil {
			return nil, err
		}
		low, err := convertExpr(n.Left)
		if err != nil {
			return nil, err
		}
		high, err := convertExpr(n.Right)
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Expr: expr, Low: low, High: high}, nil
	case *ast.PatternInExpr:
		expr, err := convertExpr(n.Expr)
		if err != nil {
			return nil, err
		}
		var vals []Expr
		for _, v := range n.List {
			e, err := convertExpr(v)
			if err != nil {
				return nil, err
			}
			vals = append(vals, e)
		}
		return &InExpr{Expr: expr, Values: vals, Not: n.Not}, nil
	case *ast.FuncCallExpr:
		var args []Expr
		for _, a := range n.Args {
			e, err := convertExpr(a)
			if err != nil {
				return nil, err
			}
			args = append(args, e)
		}
		return &FuncCallExpr{Name: n.FnName.O, Args: args}, nil
	default:
		return nil, fmt.Errorf("unsupported expr type: %T", node)
	}
}

func mustConvertExpr(node ast.ExprNode) Expr {
	expr, err := convertExpr(node)
	if err != nil {
		return &LiteralExpr{Value: nil}
	}
	return expr
}

func opToString(op opcode.Op) string {
	switch op {
	case opcode.Plus:
		return "+"
	case opcode.Minus:
		return "-"
	case opcode.Mul:
		return "*"
	case opcode.Div:
		return "/"
	case opcode.EQ:
		return "="
	case opcode.NE:
		return "!="
	case opcode.LT:
		return "<"
	case opcode.LE:
		return "<="
	case opcode.GT:
		return ">"
	case opcode.GE:
		return ">="
	case opcode.LogicAnd:
		return "AND"
	case opcode.LogicOr:
		return "OR"
	case opcode.Mod:
		return "%"
	default:
		return fmt.Sprintf("%v", op)
	}
}

func unaryOpToString(op opcode.Op) string {
	switch op {
	case opcode.Minus:
		return "-"
	case opcode.Plus:
		return "+"
	case opcode.Not:
		return "NOT"
	default:
		return fmt.Sprintf("%v", op)
	}
}

func evalLiteral(node ast.ExprNode) (any, error) {
	if v, ok := node.(ast.ValueExpr); ok {
		return v.GetValue(), nil
	}
	// Handle unary minus for negative numbers.
	if u, ok := node.(*ast.UnaryOperationExpr); ok {
		val, err := evalLiteral(u.V)
		if err != nil {
			return nil, err
		}
		switch v := val.(type) {
		case int64:
			return -v, nil
		case float64:
			return -v, nil
		case int:
			return -v, nil
		default:
			// Handle fmt.Stringer types (e.g., MyDecimal).
			if s, ok := val.(fmt.Stringer); ok {
				f, err := strconv.ParseFloat(s.String(), 64)
				if err != nil {
					return nil, fmt.Errorf("cannot parse unary operand %q: %w", s.String(), err)
				}
				return -f, nil
			}
			return nil, fmt.Errorf("unsupported unary operand: %T", val)
		}
	}
	return nil, fmt.Errorf("not a literal: %T", node)
}

func evalLiteralInt(node ast.ExprNode) (int, error) {
	v, err := evalLiteral(node)
	if err != nil {
		return 0, err
	}
	switch n := v.(type) {
	case int64:
		return int(n), nil
	case int:
		return n, nil
	case float64:
		return int(n), nil
	case uint64:
		return int(n), nil
	default:
		return 0, fmt.Errorf("not an integer: %T", v)
	}
}

// extractFromTableRefs extracts the table name from a TableRefsClause.
func extractFromTableRefs(trc *ast.TableRefsClause) string {
	if trc == nil || trc.TableRefs == nil {
		return ""
	}
	return extractTable(trc.TableRefs)
}

// extractTable extracts a table name from a Join/ResultSetNode tree.
func extractTable(node ast.ResultSetNode) string {
	switch n := node.(type) {
	case *ast.TableSource:
		if name, ok := n.Source.(*ast.TableName); ok {
			return name.Name.O
		}
	case *ast.Join:
		if t := extractTable(n.Left); t != "" {
			return t
		}
	}
	return ""
}

func getColumnName(expr ast.ExprNode) string {
	if col, ok := expr.(*ast.ColumnNameExpr); ok {
		return col.Name.Name.O
	}
	return ""
}
