package sql

import (
	"testing"

	"lns.com/minidb/catalog"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
	"lns.com/minidb/wal"
)

type testEnv struct {
	engine *storage.StorageEngine
	ts     *txn.TimestampOracle
	mgr    *txn.Manager
	cat    *catalog.Catalog
	exec   *Executor
	wal    *wal.WAL
	dir    string
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dir := t.TempDir()
	e, err := storage.OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatal(err)
	}
	ts := txn.NewTimestampOracle()
	w, _ := wal.Open(dir)
	mgr := txn.NewManager(e, ts, w)
	cat, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	exec := NewExecutor(e, mgr, cat, "testdb")
	return &testEnv{engine: e, ts: ts, mgr: mgr, cat: cat, exec: exec, wal: w, dir: dir}
}

func (env *testEnv) close() {
	env.wal.Close()
	env.cat.Close()
	env.engine.Close()
}

func TestExecCreateDBAndTable(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	// Create database.
	_, err := env.exec.Execute("CREATE DATABASE testdb")
	if err != nil {
		t.Fatal(err)
	}

	// Create table.
	_, err = env.exec.Execute(`CREATE TABLE users (
		id INT NOT NULL PRIMARY KEY,
		name VARCHAR(50)
	)`)
	if err != nil {
		t.Fatal(err)
	}

	// Verify table exists in catalog.
	td, err := env.cat.GetTable("testdb", "users")
	if err != nil {
		t.Fatal(err)
	}
	if td.Name != "users" {
		t.Errorf("expected 'users', got %q", td.Name)
	}
}

func TestExecInsertSelect(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, val INT)`)

	// Insert.
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (1, 100)")
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (2, 200)")

	// Select.
	rs, err := env.exec.Execute("SELECT id, val FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][1].(int32) != 100 {
		t.Errorf("expected val=100, got %v", rows.Rows[0][1])
	}
}

func TestExecSelectAll(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (2, 20)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (3, 30)")

	rs, err := env.exec.Execute("SELECT * FROM t1")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Rows))
	}
}

func TestExecUpdate(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	env.exec.Execute("UPDATE t1 SET v = 99 WHERE id = 1")

	rs, _ := env.exec.Execute("SELECT v FROM t1 WHERE id = 1")
	rows := rs.(*SelectResult)
	if rows.Rows[0][0].(int32) != 99 {
		t.Errorf("expected v=99, got %v", rows.Rows[0][0])
	}
}

func TestExecDelete(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (2, 20)")

	env.exec.Execute("DELETE FROM t1 WHERE id = 1")

	rs, _ := env.exec.Execute("SELECT * FROM t1")
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row after delete, got %d", len(rows.Rows))
	}
}

func TestExecTransaction(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	env.exec.Execute("BEGIN")
	env.exec.Execute("UPDATE t1 SET v = 99 WHERE id = 1")
	env.exec.Execute("COMMIT")

	rs, _ := env.exec.Execute("SELECT v FROM t1 WHERE id = 1")
	rows := rs.(*SelectResult)
	if rows.Rows[0][0].(int32) != 99 {
		t.Errorf("expected v=99 after txn, got %v", rows.Rows[0][0])
	}
}

func TestExecRollback(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	env.exec.Execute("BEGIN")
	env.exec.Execute("UPDATE t1 SET v = 99 WHERE id = 1")
	env.exec.Execute("ROLLBACK")

	rs, _ := env.exec.Execute("SELECT v FROM t1 WHERE id = 1")
	rows := rs.(*SelectResult)
	if rows.Rows[0][0].(int32) != 10 {
		t.Errorf("expected v=10 after rollback, got %v", rows.Rows[0][0])
	}
}

func TestExecShowTables(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))`)
	env.exec.Execute(`CREATE TABLE orders (id INT NOT NULL PRIMARY KEY, amount INT)`)

	rs, err := env.exec.Execute("SHOW TABLES")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(rows.Rows))
	}
}

func TestExecAutoCommit(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)

	// Without BEGIN, each statement auto-commits.
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	// Should be visible in a new implicit txn.
	rs, _ := env.exec.Execute("SELECT v FROM t1 WHERE id = 1")
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
}

func TestExplainSelect(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	rs, err := env.exec.Execute("EXPLAIN SELECT * FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Columns) != 10 {
		t.Fatalf("expected 10 columns, got %d", len(rows.Columns))
	}
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][1] != "SIMPLE" {
		t.Fatalf("expected select_type SIMPLE, got %v", rows.Rows[0][1])
	}
	if rows.Rows[0][3] != "range" {
		t.Fatalf("expected type range, got %v", rows.Rows[0][3])
	}
}

func TestExplainInsert(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)

	rs, err := env.exec.Execute("EXPLAIN INSERT INTO t1 (id, v) VALUES (1, 10)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if rows.Rows[0][1] != "INSERT" {
		t.Fatalf("expected select_type INSERT, got %v", rows.Rows[0][1])
	}
}

func TestExplainDelete(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	rs, err := env.exec.Execute("EXPLAIN DELETE FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if rows.Rows[0][1] != "DELETE" {
		t.Fatalf("expected select_type DELETE, got %v", rows.Rows[0][1])
	}
	if rows.Rows[0][3] != "range" {
		t.Fatalf("expected type range, got %v", rows.Rows[0][3])
	}
}

func TestExplainUpdate(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")

	rs, err := env.exec.Execute("EXPLAIN UPDATE t1 SET v = 20 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if rows.Rows[0][1] != "UPDATE" {
		t.Fatalf("expected select_type UPDATE, got %v", rows.Rows[0][1])
	}
	if rows.Rows[0][3] != "range" {
		t.Fatalf("expected type range, got %v", rows.Rows[0][3])
	}
}

func TestJoinSelect(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE a (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))`)
	env.exec.Execute(`CREATE TABLE b (id INT NOT NULL PRIMARY KEY, a_id INT, value INT)`)
	env.exec.Execute("INSERT INTO a (id, name) VALUES (1, 'Alice')")
	env.exec.Execute("INSERT INTO a (id, name) VALUES (2, 'Bob')")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (1, 1, 100)")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (2, 1, 200)")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (3, 2, 300)")

	rs, err := env.exec.Execute("SELECT * FROM a JOIN b ON a.id = b.a_id")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Rows))
	}
	if len(rows.Columns) != 5 {
		t.Fatalf("expected 5 columns, got %d", len(rows.Columns))
	}
}

func TestJoinSelectWithCondition(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE a (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))`)
	env.exec.Execute(`CREATE TABLE b (id INT NOT NULL PRIMARY KEY, a_id INT, value INT)`)
	env.exec.Execute("INSERT INTO a (id, name) VALUES (1, 'Alice')")
	env.exec.Execute("INSERT INTO a (id, name) VALUES (2, 'Bob')")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (1, 1, 100)")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (2, 1, 200)")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (3, 2, 300)")

	rs, err := env.exec.Execute("SELECT a.name, b.value FROM a JOIN b ON a.id = b.a_id WHERE b.value > 100")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Rows))
	}
}

func TestLeftJoinSelect(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE a (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))`)
	env.exec.Execute(`CREATE TABLE b (id INT NOT NULL PRIMARY KEY, a_id INT, value INT)`)
	env.exec.Execute("INSERT INTO a (id, name) VALUES (1, 'Alice')")
	env.exec.Execute("INSERT INTO a (id, name) VALUES (2, 'Bob')")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (1, 1, 100)")

	rs, err := env.exec.Execute("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows (including NULL row), got %d", len(rows.Rows))
	}
}

func TestRightJoinSelect(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE a (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))`)
	env.exec.Execute(`CREATE TABLE b (id INT NOT NULL PRIMARY KEY, a_id INT, value INT)`)
	env.exec.Execute("INSERT INTO a (id, name) VALUES (1, 'Alice')")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (1, 1, 100)")
	env.exec.Execute("INSERT INTO b (id, a_id, value) VALUES (2, 3, 200)")

	rs, err := env.exec.Execute("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows (including NULL row), got %d", len(rows.Rows))
	}
}

func TestSubqueryInSelect(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute(`CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (2, 20)")
	env.exec.Execute("INSERT INTO t2 (id, v) VALUES (1, 100)")

	rs, err := env.exec.Execute("SELECT * FROM t1 WHERE id = (SELECT id FROM t2 WHERE v = 100)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][0].(int32) != 1 {
		t.Fatalf("expected id=1, got %v", rows.Rows[0][0])
	}
}

func TestSubqueryInIn(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute(`CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (2, 20)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (3, 30)")
	env.exec.Execute("INSERT INTO t2 (id, v) VALUES (1, 100)")
	env.exec.Execute("INSERT INTO t2 (id, v) VALUES (2, 200)")

	rs, err := env.exec.Execute("SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Rows))
	}
}

func TestExistsSubquery(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute(`CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (2, 20)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (3, 30)")
	env.exec.Execute("INSERT INTO t2 (id, v) VALUES (1, 100)")

	rs, err := env.exec.Execute("SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
}

func TestNotExistsSubquery(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute(`CREATE TABLE t2 (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (2, 20)")
	env.exec.Execute("INSERT INTO t1 (id, v) VALUES (3, 30)")
	env.exec.Execute("INSERT INTO t2 (id, v) VALUES (1, 100)")

	rs, err := env.exec.Execute("SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Rows))
	}
}
