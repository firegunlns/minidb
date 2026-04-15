package catalog

import (
	"testing"

	"lns.com/minidb/storage"
)

func tempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	return dir
}

func TestBootstrap(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// After bootstrap, catalog should be usable.
	dbs, err := c.ListDatabases()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbs) != 0 {
		t.Fatalf("expected 0 databases, got %d", len(dbs))
	}
}

func TestCreateDatabase(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.CreateDatabase("testdb"); err != nil {
		t.Fatal(err)
	}
	dbs, err := c.ListDatabases()
	if err != nil {
		t.Fatal(err)
	}
	if len(dbs) != 1 || dbs[0] != "testdb" {
		t.Fatalf("expected [testdb], got %v", dbs)
	}
}

func TestCreateTable(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.CreateDatabase("mydb")

	td := &TableDef{
		Database: "mydb",
		Name:     "users",
		Columns: []storage.ColumnDef{
			{Name: "id", Type: storage.ColTypeInt},
			{Name: "name", Type: storage.ColTypeVarchar, Length: 50},
			{Name: "balance", Type: storage.ColTypeDecimal, Precision: 10, Scale: 2},
		},
		PKCols: []int{0},
	}
	if err := c.CreateTable(td); err != nil {
		t.Fatal(err)
	}

	got, err := c.GetTable("mydb", "users")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "users" {
		t.Fatalf("expected table name 'users', got %q", got.Name)
	}
	if len(got.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(got.Columns))
	}
	if got.Columns[0].Name != "id" {
		t.Errorf("expected first column 'id', got %q", got.Columns[0].Name)
	}
}

func TestCatalogPersistence(t *testing.T) {
	dir := tempDir(t)

	// Create catalog and add entries.
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	c.CreateDatabase("db1")
	c.CreateTable(&TableDef{
		Database: "db1",
		Name:     "t1",
		Columns: []storage.ColumnDef{
			{Name: "pk", Type: storage.ColTypeInt},
			{Name: "val", Type: storage.ColTypeVarchar, Length: 10},
		},
		PKCols: []int{0},
	})
	c.Close()

	// Reopen and verify.
	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	dbs, _ := c2.ListDatabases()
	if len(dbs) != 1 || dbs[0] != "db1" {
		t.Fatalf("expected [db1], got %v", dbs)
	}

	got, err := c2.GetTable("db1", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "t1" || len(got.Columns) != 2 {
		t.Fatalf("unexpected table: %+v", got)
	}
}

func TestListTables(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.CreateDatabase("db1")
	c.CreateTable(&TableDef{Database: "db1", Name: "a", Columns: []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}}, PKCols: []int{0}})
	c.CreateTable(&TableDef{Database: "db1", Name: "b", Columns: []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}}, PKCols: []int{0}})

	tables, err := c.ListTables("db1")
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(tables))
	}
}

func TestAutoInc(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	id1, err := c.NextAutoInc("mydb", "orders", "id")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := c.NextAutoInc("mydb", "orders", "id")
	if err != nil {
		t.Fatal(err)
	}
	if id2 <= id1 {
		t.Fatalf("expected incrementing IDs: %d <= %d", id2, id1)
	}
}

func TestDropTable(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.CreateDatabase("db1")
	c.CreateTable(&TableDef{Database: "db1", Name: "temp", Columns: []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}}, PKCols: []int{0}})

	if err := c.DropTable("db1", "temp"); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetTable("db1", "temp")
	if err == nil {
		t.Fatal("expected error for dropped table")
	}
}

func TestDropDatabase(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.CreateDatabase("db1")
	c.CreateTable(&TableDef{Database: "db1", Name: "t", Columns: []storage.ColumnDef{{Name: "id", Type: storage.ColTypeInt}}, PKCols: []int{0}})

	if err := c.DropDatabase("db1"); err != nil {
		t.Fatal(err)
	}
	dbs, _ := c.ListDatabases()
	if len(dbs) != 0 {
		t.Fatalf("expected 0 databases, got %v", dbs)
	}
}

func TestTableWithIndexes(t *testing.T) {
	dir := tempDir(t)
	c, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	c.CreateDatabase("db1")
	td := &TableDef{
		Database: "db1",
		Name:     "customer",
		Columns: []storage.ColumnDef{
			{Name: "w_id", Type: storage.ColTypeInt},
			{Name: "d_id", Type: storage.ColTypeInt},
			{Name: "c_id", Type: storage.ColTypeInt},
			{Name: "c_last", Type: storage.ColTypeVarchar, Length: 16},
		},
		PKCols: []int{0, 1, 2},
		Indexes: []IndexDef{
			{Name: "idx_name", Columns: []string{"w_id", "d_id", "c_last"}, Unique: false},
		},
	}
	if err := c.CreateTable(td); err != nil {
		t.Fatal(err)
	}

	got, err := c.GetTable("db1", "customer")
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(got.Indexes))
	}
	if got.Indexes[0].Name != "idx_name" {
		t.Errorf("expected index 'idx_name', got %q", got.Indexes[0].Name)
	}
	// Verify persistence.
	c.Close()

	c2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	got2, err := c2.GetTable("db1", "customer")
	if err != nil {
		t.Fatal(err)
	}
	if len(got2.Indexes) != 1 {
		t.Fatalf("after reopen: expected 1 index, got %d", len(got2.Indexes))
	}
}
