package sql

import (
	"testing"
)

func TestParseCreateDatabase(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("CREATE DATABASE testdb")
	if err != nil {
		t.Fatal(err)
	}
	cd, ok := stmt.(*CreateDatabaseStmt)
	if !ok {
		t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
	}
	if cd.Name != "testdb" {
		t.Errorf("expected 'testdb', got %q", cd.Name)
	}
}

func TestParseUse(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("USE mydb")
	if err != nil {
		t.Fatal(err)
	}
	u, ok := stmt.(*UseStmt)
	if !ok {
		t.Fatalf("expected *UseStmt, got %T", stmt)
	}
	if u.DBName != "mydb" {
		t.Errorf("expected 'mydb', got %q", u.DBName)
	}
}

func TestParseCreateTable(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse(`CREATE TABLE users (
		id INT NOT NULL PRIMARY KEY,
		name VARCHAR(50),
		balance DECIMAL(10,2)
	)`)
	if err != nil {
		t.Fatal(err)
	}
	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}
	if ct.Table != "users" {
		t.Errorf("expected 'users', got %q", ct.Table)
	}
	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}
	if ct.Columns[0].Name != "id" {
		t.Errorf("expected first col 'id', got %q", ct.Columns[0].Name)
	}
	if ct.Columns[1].Name != "name" {
		t.Errorf("expected second col 'name', got %q", ct.Columns[1].Name)
	}
}

func TestParseInsert(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("INSERT INTO users (id, name) VALUES (1, 'Alice')")
	if err != nil {
		t.Fatal(err)
	}
	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}
	if ins.Table != "users" {
		t.Errorf("expected 'users', got %q", ins.Table)
	}
	if len(ins.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ins.Columns))
	}
	if len(ins.Values) != 1 {
		t.Fatalf("expected 1 value row, got %d", len(ins.Values))
	}
}

func TestParseSelect(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("SELECT id, name FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	ref, ok := sel.TableRef.(*SimpleTableRef)
	if !ok {
		t.Fatalf("expected *SimpleTableRef, got %T", sel.TableRef)
	}
	if ref.Table != "users" {
		t.Errorf("expected table 'users', got %q", ref.Table)
	}
	if len(sel.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(sel.Columns))
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE clause")
	}
}

func TestParseUpdate(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}
	if upd.Table != "users" {
		t.Errorf("expected 'users', got %q", upd.Table)
	}
	if len(upd.SetClauses) != 1 {
		t.Fatalf("expected 1 SET clause, got %d", len(upd.SetClauses))
	}
}

func TestParseDelete(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("DELETE FROM users WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", stmt)
	}
	if del.Table != "users" {
		t.Errorf("expected 'users', got %q", del.Table)
	}
}

func TestParseBeginCommit(t *testing.T) {
	p := NewParser()
	begin, err := p.Parse("BEGIN")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := begin.(*BeginStmt); !ok {
		t.Fatalf("expected *BeginStmt, got %T", begin)
	}

	commit, err := p.Parse("COMMIT")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := commit.(*CommitStmt); !ok {
		t.Fatalf("expected *CommitStmt, got %T", commit)
	}
}

func TestParseRollback(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("ROLLBACK")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*RollbackStmt); !ok {
		t.Fatalf("expected *RollbackStmt, got %T", stmt)
	}
}

func TestParseDropTable(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("DROP TABLE users")
	if err != nil {
		t.Fatal(err)
	}
	dt, ok := stmt.(*DropTableStmt)
	if !ok {
		t.Fatalf("expected *DropTableStmt, got %T", stmt)
	}
	if dt.Table != "users" {
		t.Errorf("expected 'users', got %q", dt.Table)
	}
}

func TestParseSelectStar(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if !sel.SelectAll {
		t.Error("expected SelectAll=true")
	}
}

func TestParseSelectOrderBy(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("SELECT * FROM users ORDER BY name LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}
	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}
	if len(sel.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(sel.OrderBy))
	}
	if sel.Limit == nil {
		t.Fatal("expected LIMIT")
	}
}

func TestParseShowTables(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := stmt.(*ShowTablesStmt); !ok {
		t.Fatalf("expected *ShowTablesStmt, got %T", stmt)
	}
}

func TestParseAlterTableAddForeignKey(t *testing.T) {
	p := NewParser()
	stmt, err := p.Parse("alter table bmsql_district add constraint d_warehouse_fkey foreign key (d_w_id) references bmsql_warehouse (w_id)")
	if err != nil {
		t.Fatal(err)
	}
	at, ok := stmt.(*AlterTableStmt)
	if !ok {
		t.Fatalf("expected *AlterTableStmt, got %T", stmt)
	}
	if at.Table != "bmsql_district" {
		t.Errorf("expected table 'bmsql_district', got %q", at.Table)
	}
	if len(at.Specs) != 1 {
		t.Fatalf("expected 1 spec, got %d", len(at.Specs))
	}
	spec := at.Specs[0]
	if spec.Type != AlterAddConstraint {
		t.Errorf("expected AlterAddConstraint, got %d", spec.Type)
	}
	if spec.Constraint == nil {
		t.Fatal("expected constraint")
	}
	if spec.Constraint.Type != ConstraintTypeForeignKey {
		t.Errorf("expected ConstraintTypeForeignKey, got %d", spec.Constraint.Type)
	}
	if spec.Constraint.Name != "d_warehouse_fkey" {
		t.Errorf("expected constraint name 'd_warehouse_fkey', got %q", spec.Constraint.Name)
	}
	if len(spec.Constraint.Keys) != 1 || spec.Constraint.Keys[0] != "d_w_id" {
		t.Errorf("expected key 'd_w_id', got %v", spec.Constraint.Keys)
	}
	if spec.Constraint.ReferTable != "bmsql_warehouse" {
		t.Errorf("expected ref table 'bmsql_warehouse', got %q", spec.Constraint.ReferTable)
	}
	if len(spec.Constraint.ReferKeys) != 1 || spec.Constraint.ReferKeys[0] != "w_id" {
		t.Errorf("expected ref key 'w_id', got %v", spec.Constraint.ReferKeys)
	}
}
