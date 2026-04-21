package sql

import (
	"fmt"
	"strings"
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
	mgr := txn.NewManager(e, ts, w, 0)
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

func TestLikeBasic(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (1, 'hello world')`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (2, 'goodbye world')`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (3, 'hello there')`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (4, 'foo bar')`)

	rs, err := env.exec.Execute("SELECT * FROM t WHERE name LIKE '%hello%'")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Rows))
	}

	rs, err = env.exec.Execute("SELECT * FROM t WHERE name LIKE 'hello%'")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows for prefix match, got %d", len(rows.Rows))
	}

	rs, err = env.exec.Execute("SELECT * FROM t WHERE name LIKE '%world'")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows for suffix match, got %d", len(rows.Rows))
	}

	rs, err = env.exec.Execute("SELECT * FROM t WHERE name LIKE 'hello world'")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row for exact match, got %d", len(rows.Rows))
	}

	rs, err = env.exec.Execute("SELECT * FROM t WHERE name LIKE '___ bar'")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row for _ wildcard, got %d", len(rows.Rows))
	}
}

func TestNotLike(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t (id INT NOT NULL PRIMARY KEY, name VARCHAR(100))`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (1, 'hello world')`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (2, 'goodbye world')`)
	env.exec.Execute(`INSERT INTO t (id, name) VALUES (3, 'foo bar')`)

	rs, err := env.exec.Execute("SELECT * FROM t WHERE name NOT LIKE '%hello%'")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows for NOT LIKE, got %d", len(rows.Rows))
	}
}

func TestAggregateCount(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t (id INT NOT NULL PRIMARY KEY, v INT)`)
	env.exec.Execute(`INSERT INTO t (id, v) VALUES (1, 10)`)
	env.exec.Execute(`INSERT INTO t (id, v) VALUES (2, 20)`)
	env.exec.Execute(`INSERT INTO t (id, v) VALUES (3, 30)`)

	rs, err := env.exec.Execute("SELECT COUNT(*) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][0].(int64) != 3 {
		t.Fatalf("expected count=3, got %v", rows.Rows[0][0])
	}

	rs, err = env.exec.Execute("SELECT COUNT(1) FROM t")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if rows.Rows[0][0].(int64) != 3 {
		t.Fatalf("expected count=3, got %v", rows.Rows[0][0])
	}

	rs, err = env.exec.Execute("SELECT COUNT(*) FROM t WHERE id > 1")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if rows.Rows[0][0].(int64) != 2 {
		t.Fatalf("expected count=2, got %v", rows.Rows[0][0])
	}
}

// TestSelectINOnPK verifies that WHERE col IN (...) on a single-column PK
// uses point lookups instead of a full table scan.
func TestSelectINOnPK(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE item (i_id INT NOT NULL PRIMARY KEY, i_name VARCHAR(24), i_price DECIMAL(5,2))`)

	// Insert 20 items.
	for i := 1; i <= 20; i++ {
		env.exec.Execute(fmt.Sprintf("INSERT INTO item (i_id, i_name, i_price) VALUES (%d, 'item_%d', '%d.00')", i, i, i*10))
	}

	// Single-value IN.
	rs, err := env.exec.Execute("SELECT i_id, i_name FROM item WHERE i_id IN (5)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("IN (5): expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][0].(int32) != 5 {
		t.Errorf("IN (5): expected i_id=5, got %v", rows.Rows[0][0])
	}

	// Multi-value IN.
	rs, err = env.exec.Execute("SELECT i_id, i_name FROM item WHERE i_id IN (3, 7, 11)")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 3 {
		t.Fatalf("IN (3,7,11): expected 3 rows, got %d", len(rows.Rows))
	}

	// IN with non-existent value.
	rs, err = env.exec.Execute("SELECT i_id FROM item WHERE i_id IN (99)")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 0 {
		t.Fatalf("IN (99): expected 0 rows, got %d", len(rows.Rows))
	}

	// Mixed: some exist, some don't.
	rs, err = env.exec.Execute("SELECT i_id FROM item WHERE i_id IN (1, 99, 20)")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("IN (1,99,20): expected 2 rows, got %d", len(rows.Rows))
	}
}

// TestSelectINOnNonPKColumn verifies that IN on a non-PK column
// falls back to a full scan (doesn't use the PK optimization).
func TestSelectINOnNonPKColumn(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, val INT)`)

	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (2, 20)")
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (3, 10)")

	rs, err := env.exec.Execute("SELECT id FROM t1 WHERE val IN (10)")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("val IN (10): expected 2 rows, got %d", len(rows.Rows))
	}
}

// TestSecondaryIndex tests CREATE INDEX + query + INSERT/UPDATE/DELETE index maintenance.
func TestSecondaryIndex(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE customer (
		c_w_id INT NOT NULL,
		c_d_id INT NOT NULL,
		c_id INT NOT NULL,
		c_last VARCHAR(16),
		c_first VARCHAR(16),
		c_credit VARCHAR(2),
		PRIMARY KEY (c_w_id, c_d_id, c_id)
	)`)

	// Create secondary index.
	_, err := env.exec.Execute("CREATE INDEX idx_c_last ON customer (c_w_id, c_d_id, c_last, c_first)")
	if err != nil {
		t.Fatal(err)
	}

	// Insert data.
	for w := 1; w <= 2; w++ {
		for d := 1; d <= 2; d++ {
			for c := 1; c <= 5; c++ {
				last := fmt.Sprintf("LAST_%d", c%3)
				first := fmt.Sprintf("FIRST_%d", c)
				env.exec.Execute(fmt.Sprintf(
					"INSERT INTO customer (c_w_id, c_d_id, c_id, c_last, c_first, c_credit) VALUES (%d, %d, %d, '%s', '%s', 'GC')",
					w, d, c, last, first))
			}
		}
	}

	// Query by c_last — should use index.
	rs, err := env.exec.Execute("SELECT c_id, c_first FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'LAST_1' ORDER BY c_first")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	t.Logf("Index scan result: %v", rows.Rows)
	// LAST_1: c_id % 3 == 1 => c_id=1, c_id=4 → 2 rows
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Rows))
	}
	// Should be ordered by c_first.
	if rows.Rows[0][1].(string) > rows.Rows[1][1].(string) {
		t.Error("results not ordered by c_first")
	}

	// Update an indexed column.
	env.exec.Execute("UPDATE customer SET c_last = 'LAST_0' WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1")

	// Old value should be gone.
	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'LAST_1' ORDER BY c_first")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 { // was 2, now 1 after updating c_id=1
		t.Errorf("after UPDATE: expected 1 row for LAST_1, got %d", len(rows.Rows))
	}

	// New value should be found.
	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'LAST_0' ORDER BY c_first")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 2 { // c_id=1 (updated) + c_id=3 (original LAST_0)
		t.Errorf("after UPDATE: expected 2 rows for LAST_0, got %d", len(rows.Rows))
	}

	// Delete a row.
	env.exec.Execute("DELETE FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 3")

	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'LAST_0'")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 { // c_id=1 only
		t.Errorf("after DELETE: expected 1 row, got %d", len(rows.Rows))
	}
}

// TestCreateIndexWithBackfill tests CREATE INDEX on a table with existing data.
func TestCreateIndexWithBackfill(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE items (id INT NOT NULL PRIMARY KEY, name VARCHAR(20), category INT)`)

	// Insert data BEFORE creating index.
	for i := 1; i <= 10; i++ {
		env.exec.Execute(fmt.Sprintf("INSERT INTO items (id, name, category) VALUES (%d, 'item_%d', %d)", i, i, i%3))
	}

	// Now create index on existing data.
	_, err := env.exec.Execute("CREATE INDEX idx_category ON items (category)")
	if err != nil {
		t.Fatal(err)
	}

	// Query should use index.
	rs, err := env.exec.Execute("SELECT id FROM items WHERE category = 0")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	// category=0: id=3, id=6, id=9 → 3 rows
	if len(rows.Rows) != 3 {
		t.Fatalf("expected 3 rows with category=0, got %d", len(rows.Rows))
	}
}

// TestInlineIndexInCreateTable tests KEY constraint in CREATE TABLE.
func TestInlineIndexInCreateTable(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE t1 (
		id INT NOT NULL PRIMARY KEY,
		val INT,
		KEY idx_val (val)
	)`)

	// Verify index was created.
	td, _ := env.cat.GetTable("testdb", "t1")
	if len(td.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(td.Indexes))
	}
	if td.Indexes[0].Name != "idx_val" {
		t.Errorf("expected index name 'idx_val', got %q", td.Indexes[0].Name)
	}

	// Insert and query.
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (1, 10)")
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (2, 10)")
	env.exec.Execute("INSERT INTO t1 (id, val) VALUES (3, 20)")

	rs, _ := env.exec.Execute("SELECT id FROM t1 WHERE val = 10")
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows.Rows))
	}
}

// TestShowIndex tests SHOW INDEX and SHOW KEYS FROM.
func TestShowIndex(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE customer (
		c_w_id INT NOT NULL,
		c_d_id INT NOT NULL,
		c_id INT NOT NULL,
		c_last VARCHAR(16),
		c_first VARCHAR(16),
		PRIMARY KEY (c_w_id, c_d_id, c_id)
	)`)
	env.exec.Execute("CREATE INDEX idx_c_last ON customer (c_w_id, c_d_id, c_last, c_first)")

	// SHOW INDEX FROM
	rs, err := env.exec.Execute("SHOW INDEX FROM customer")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	// 3 PK columns + 4 index columns = 7 rows
	if len(rows.Rows) != 7 {
		t.Fatalf("expected 7 rows, got %d", len(rows.Rows))
	}
	// Verify columns.
	if len(rows.Columns) != 15 {
		t.Fatalf("expected 15 columns, got %d", len(rows.Columns))
	}
	// Check PK rows.
	for i := 0; i < 3; i++ {
		if rows.Rows[i][2] != "PRIMARY" {
			t.Errorf("row %d: expected Key_name=PRIMARY, got %v", i, rows.Rows[i][2])
		}
		if rows.Rows[i][1] != int32(0) {
			t.Errorf("row %d: expected Non_unique=0, got %v", i, rows.Rows[i][1])
		}
	}
	// Check secondary index rows.
	for i := 3; i < 7; i++ {
		if rows.Rows[i][2] != "idx_c_last" {
			t.Errorf("row %d: expected Key_name=idx_c_last, got %v", i, rows.Rows[i][2])
		}
		if rows.Rows[i][1] != int32(1) {
			t.Errorf("row %d: expected Non_unique=1, got %v", i, rows.Rows[i][1])
		}
	}
	// Check column order in secondary index.
	expectedCols := []string{"c_w_id", "c_d_id", "c_last", "c_first"}
	for i, col := range expectedCols {
		if rows.Rows[3+i][4] != col {
			t.Errorf("idx row %d: expected Column_name=%s, got %v", i, col, rows.Rows[3+i][4])
		}
		if rows.Rows[3+i][3] != int32(i+1) {
			t.Errorf("idx row %d: expected Seq_in_index=%d, got %v", i, i+1, rows.Rows[3+i][3])
		}
	}

	// SHOW KEYS FROM (same output).
	rs2, err := env.exec.Execute("SHOW KEYS FROM customer")
	if err != nil {
		t.Fatal(err)
	}
	rows2 := rs2.(*SelectResult)
	if len(rows2.Rows) != len(rows.Rows) {
		t.Fatalf("SHOW KEYS: expected %d rows, got %d", len(rows.Rows), len(rows2.Rows))
	}
}

// TestExplainWithIndex tests that EXPLAIN reports secondary index usage.
func TestExplainWithIndex(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE customer (
		c_w_id INT NOT NULL,
		c_d_id INT NOT NULL,
		c_id INT NOT NULL,
		c_last VARCHAR(16),
		c_first VARCHAR(16),
		PRIMARY KEY (c_w_id, c_d_id, c_id)
	)`)
	env.exec.Execute("CREATE INDEX idx_c_last ON customer (c_w_id, c_d_id, c_last, c_first)")

	// EXPLAIN a query that uses the secondary index.
	rs, err := env.exec.Execute("EXPLAIN SELECT c_id, c_first FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR' ORDER BY c_first")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}

	row := rows.Rows[0]
	// possible_keys should list both PRIMARY and the index (comma-separated).
	possibleKeys, ok := row[4].(string)
	if !ok {
		t.Fatalf("expected string for possible_keys, got %T: %v", row[4], row[4])
	}
	if !strings.Contains(possibleKeys, "PRIMARY") || !strings.Contains(possibleKeys, "idx_c_last") {
		t.Errorf("expected possible_keys to contain PRIMARY and idx_c_last, got %v", possibleKeys)
	}

	// key should be idx_c_last (best match with 3 equalities).
	if row[5] != "idx_c_last" {
		t.Errorf("expected key=idx_c_last, got %v", row[5])
	}

	// Extra should mention "using index" (covering) and NOT "using filesort" (ORDER BY satisfied).
	extra := row[9].(string)
	t.Logf("Extra: %s", extra)
	if !strings.Contains(extra, "using index") {
		t.Errorf("expected Extra to contain 'using index', got %q", extra)
	}
	if strings.Contains(extra, "using filesort") {
		t.Errorf("expected Extra NOT to contain 'using filesort', got %q", extra)
	}

	// EXPLAIN UPDATE with index.
	rs, err = env.exec.Execute("EXPLAIN UPDATE customer SET c_first = 'X' WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 5")
	if err != nil {
		t.Fatal(err)
	}
	rows = rs.(*SelectResult)
	row = rows.Rows[0]
	if row[5] != "PRIMARY" {
		t.Errorf("UPDATE: expected key=PRIMARY (PK match), got %v", row[5])
	}

	// EXPLAIN with no WHERE — should be ALL.
	rs, _ = env.exec.Execute("EXPLAIN SELECT * FROM customer")
	rows = rs.(*SelectResult)
	row = rows.Rows[0]
	if row[3] != "ALL" {
		t.Errorf("no WHERE: expected type=ALL, got %v", row[3])
	}
	if row[5] != nil {
		t.Errorf("no WHERE: expected key=nil, got %v", row[5])
	}
}

// TestSecondaryIndexEndToEnd comprehensively tests that:
// 1. Index entries are actually written to the B+ tree (file grows)
// 2. Queries use the index (not full table scan)
// 3. EXPLAIN shows correct index usage
// 4. Inline KEY in CREATE TABLE works
// 5. UPDATE/DELETE maintain index correctness
// 6. Index with nil-ish values (empty string, zero) works
func TestSecondaryIndexEndToEnd(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")

	// Create table with inline index — mimics TPC-C customer.
	env.exec.Execute(`CREATE TABLE customer (
		c_w_id INT NOT NULL,
		c_d_id INT NOT NULL,
		c_id INT NOT NULL,
		c_discount DECIMAL(4,4),
		c_credit CHAR(2),
		c_last VARCHAR(16),
		c_first VARCHAR(16),
		c_balance DECIMAL(12,2),
		PRIMARY KEY (c_w_id, c_d_id, c_id),
		KEY idx_c_last (c_w_id, c_d_id, c_last, c_first)
	)`)

	// Verify index is in table definition.
	td, err := env.cat.GetTable("testdb", "customer")
	if err != nil {
		t.Fatal(err)
	}
	if len(td.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(td.Indexes))
	}
	if td.Indexes[0].Name != "idx_c_last" {
		t.Errorf("expected idx_c_last, got %s", td.Indexes[0].Name)
	}

	// Insert data — 3 warehouses x 2 districts x 10 customers = 60 rows.
	// c_last cycles through BAR, BAT, BAR, ... so BAR customers: odd c_ids.
	for w := 1; w <= 3; w++ {
		for d := 1; d <= 2; d++ {
			for c := 1; c <= 10; c++ {
				last := "BAT"
				if c%2 == 1 {
					last = "BAR"
				}
				env.exec.Execute(fmt.Sprintf(
					"INSERT INTO customer (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance) VALUES (%d, %d, %d, 0.10, 'GC', '%s', 'FIRST_%d', 100.00)",
					w, d, c, last, c))
			}
		}
	}

	// --- Step 1: Verify index file has data (not just header) ---
	idxTreeKey := td.IndexFile(&td.Indexes[0])
	// Scan the index tree directly to verify entries exist.
	idxCount := 0
	env.engine.ScanRaw(idxTreeKey, []byte{0x00}, []byte{0xFF}, func(key, value []byte) bool {
		idxCount++
		return true
	})
	if idxCount != 60 {
		t.Errorf("expected 60 index entries, got %d", idxCount)
	}
	t.Logf("Index tree has %d entries", idxCount)

	// --- Step 2: Query by c_last should use index ---
	rs, err := env.exec.Execute("SELECT c_id, c_first FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR' ORDER BY c_first")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	// BAR: c_id=1,3,5,7,9 → 5 rows
	if len(rows.Rows) != 5 {
		t.Fatalf("expected 5 rows, got %d: %v", len(rows.Rows), rows.Rows)
	}
	// Should be ordered by c_first (FIRST_1, FIRST_3, FIRST_5, FIRST_7, FIRST_9).
	for i, row := range rows.Rows {
		expected := fmt.Sprintf("FIRST_%d", 1+2*i)
		if row[1].(string) != expected {
			t.Errorf("row %d: expected c_first=%s, got %v", i, expected, row[1])
		}
	}

	// --- Step 3: EXPLAIN should show index usage ---
	rs, err = env.exec.Execute("EXPLAIN SELECT c_id, c_first FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR' ORDER BY c_first")
	if err != nil {
		t.Fatal(err)
	}
	explainRows := rs.(*SelectResult)
	if len(explainRows.Rows) != 1 {
		t.Fatal("expected 1 explain row")
	}
	explainRow := explainRows.Rows[0]
	possibleKeys := explainRow[4].(string)
	usedKey := explainRow[5].(string)
	extra := explainRow[9].(string)

	if !strings.Contains(possibleKeys, "idx_c_last") {
		t.Errorf("possible_keys should contain idx_c_last, got %q", possibleKeys)
	}
	if usedKey != "idx_c_last" {
		t.Errorf("key should be idx_c_last, got %q", usedKey)
	}
	if !strings.Contains(extra, "using index") {
		t.Errorf("extra should contain 'using index', got %q", extra)
	}
	if strings.Contains(extra, "using filesort") {
		t.Errorf("extra should NOT contain 'using filesort', got %q", extra)
	}
	t.Logf("EXPLAIN: type=%v possible_keys=%v key=%v extra=%v", explainRow[3], possibleKeys, usedKey, extra)

	// --- Step 4: PK point query should prefer PRIMARY over secondary index ---
	rs, _ = env.exec.Execute("EXPLAIN SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 5")
	explainRows = rs.(*SelectResult)
	explainRow = explainRows.Rows[0]
	if explainRow[5] != "PRIMARY" {
		t.Errorf("PK point query should use PRIMARY, got key=%v", explainRow[5])
	}

	// --- Step 5: UPDATE an indexed column, verify index is maintained ---
	env.exec.Execute("UPDATE customer SET c_last = 'BAZ' WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1")

	// Old index entry should be gone.
	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR' ORDER BY c_first")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 4 { // was 5, c_id=1 moved to BAZ
		t.Errorf("after UPDATE: expected 4 BAR rows, got %d", len(rows.Rows))
	}

	// New index entry should exist.
	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAZ'")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Errorf("after UPDATE: expected 1 BAZ row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][0].(int32) != 1 {
		t.Errorf("after UPDATE: expected c_id=1, got %v", rows.Rows[0][0])
	}

	// Verify total index entries still = 60.
	idxCount2 := 0
	env.engine.ScanRaw(idxTreeKey, []byte{0x00}, []byte{0xFF}, func(key, value []byte) bool {
		idxCount2++
		return true
	})
	if idxCount2 != 60 {
		t.Errorf("after UPDATE: expected 60 index entries, got %d", idxCount2)
	}

	// --- Step 6: DELETE a row, verify index entry is removed ---
	env.exec.Execute("DELETE FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 3")

	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAR'")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 3 { // was 4, c_id=3 deleted
		t.Errorf("after DELETE: expected 3 BAR rows, got %d", len(rows.Rows))
	}

	idxCount3 := 0
	env.engine.ScanRaw(idxTreeKey, []byte{0x00}, []byte{0xFF}, func(key, value []byte) bool {
		idxCount3++
		return true
	})
	if idxCount3 != 59 {
		t.Errorf("after DELETE: expected 59 index entries, got %d", idxCount3)
	}

	// --- Step 7: Non-covering index query (SELECT *) ---
	rs, _ = env.exec.Execute("SELECT c_id, c_last FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'BAZ'")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Errorf("expected 1 BAZ row, got %d", len(rows.Rows))
	}

	// --- Step 8: Query with no matching rows ---
	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_last = 'NONEXISTENT'")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 0 {
		t.Errorf("expected 0 rows for NONEXISTENT, got %d", len(rows.Rows))
	}

	// --- Step 9: Full table scan when index can't help ---
	rs, _ = env.exec.Execute("SELECT c_id FROM customer WHERE c_last = 'BAR'")
	rows = rs.(*SelectResult)
	// Can't use index (missing c_w_id, c_d_id prefix), falls back to scan.
	totalBAR := 0
	for w := 1; w <= 3; w++ {
		for d := 1; d <= 2; d++ {
			for c := 1; c <= 10; c++ {
				if c%2 == 1 {
					totalBAR++
				}
			}
		}
	}
	// But c_id=1 was changed to BAZ, c_id=3 in w=1,d=1 was deleted
	totalBAR -= 2
	if len(rows.Rows) != totalBAR {
		t.Errorf("expected %d BAR rows (full scan), got %d", totalBAR, len(rows.Rows))
	}
}

// TestOorderUniqueIndex tests the oorder unique index from TPC-C.
func TestOorderUniqueIndex(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")
	env.exec.Execute(`CREATE TABLE bmsql_oorder (
		o_w_id INT NOT NULL,
		o_d_id INT NOT NULL,
		o_id INT NOT NULL,
		o_c_id INT,
		o_carrier_id INT,
		PRIMARY KEY (o_w_id, o_d_id, o_id),
		UNIQUE KEY bmsql_oorder_idx1 (o_w_id, o_d_id, o_c_id, o_id)
	)`)

	// Verify index was created.
	td, _ := env.cat.GetTable("testdb", "bmsql_oorder")
	if len(td.Indexes) != 1 {
		t.Fatalf("expected 1 index, got %d", len(td.Indexes))
	}
	if !td.Indexes[0].Unique {
		t.Error("expected unique index")
	}

	// Insert orders.
	for w := 1; w <= 2; w++ {
		for d := 1; d <= 2; d++ {
			for o := 1; o <= 5; o++ {
				env.exec.Execute(fmt.Sprintf(
					"INSERT INTO bmsql_oorder (o_w_id, o_d_id, o_id, o_c_id, o_carrier_id) VALUES (%d, %d, %d, %d, NULL)",
					w, d, o, o*10))
			}
		}
	}

	// Query using unique index columns.
	rs, err := env.exec.Execute("SELECT o_id FROM bmsql_oorder WHERE o_w_id = 1 AND o_d_id = 1 AND o_c_id = 30")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][0].(int32) != 3 {
		t.Errorf("expected o_id=3, got %v", rows.Rows[0][0])
	}

	// Verify index entries count.
	idxTreeKey := td.IndexFile(&td.Indexes[0])
	idxCount := 0
	env.engine.ScanRaw(idxTreeKey, []byte{0x00}, []byte{0xFF}, func(key, value []byte) bool {
		idxCount++
		return true
	})
	if idxCount != 20 {
		t.Errorf("expected 20 index entries, got %d", idxCount)
	}

	// EXPLAIN should show index usage.
	rs, _ = env.exec.Execute("EXPLAIN SELECT o_id FROM bmsql_oorder WHERE o_w_id = 1 AND o_d_id = 1 AND o_c_id = 30")
	explainRows := rs.(*SelectResult)
	explainRow := explainRows.Rows[0]
	t.Logf("EXPLAIN oorder: type=%v possible_keys=%v key=%v extra=%v",
		explainRow[3], explainRow[4], explainRow[5], explainRow[9])
}

// TestCustomerIndexSimulatedTPCC simulates the exact TPC-C Payment c_last query
// to verify the index is being used.
func TestCustomerIndexSimulatedTPCC(t *testing.T) {
	env := newTestEnv(t)
	defer env.close()

	env.exec.Execute("CREATE DATABASE testdb")

	// Exact TPC-C customer table with inline index.
	env.exec.Execute(`CREATE TABLE bmsql_customer (
		c_w_id INT NOT NULL,
		c_d_id INT NOT NULL,
		c_id INT NOT NULL,
		c_discount DECIMAL(4,4),
		c_credit CHAR(2),
		c_last VARCHAR(16),
		c_first VARCHAR(16),
		c_credit_lim DECIMAL(12,2),
		c_balance DECIMAL(12,2),
		c_ytd_payment DECIMAL(12,2),
		c_payment_cnt INT,
		c_delivery_cnt INT,
		c_street_1 VARCHAR(20),
		c_street_2 VARCHAR(20),
		c_city VARCHAR(20),
		c_state CHAR(2),
		c_zip CHAR(9),
		c_phone CHAR(16),
		c_since TIMESTAMP,
		c_middle CHAR(2),
		c_data VARCHAR(500),
		PRIMARY KEY (c_w_id, c_d_id, c_id),
		KEY bmsql_customer_idx1 (c_w_id, c_d_id, c_last, c_first)
	)`)

	td, _ := env.cat.GetTable("testdb", "bmsql_customer")
	t.Logf("Indexes: %d, name=%s, cols=%v", len(td.Indexes), td.Indexes[0].Name, td.Indexes[0].Columns)

	// Insert 30 customers (1 warehouse, 10 districts, 3 per district).
	// c_last = "BAR" for c_id 1..15, "BAT" for c_id 16..30
	for d := 1; d <= 10; d++ {
		for c := 1; c <= 3; c++ {
			id := (d-1)*3 + c
			last := "BAR"
			if id > 15 {
				last = "BAT"
			}
			env.exec.Execute(fmt.Sprintf(
				"INSERT INTO bmsql_customer (c_w_id, c_d_id, c_id, c_discount, c_credit, c_last, c_first, c_balance) VALUES (1, %d, %d, 0.1234, 'GC', '%s', 'FIRST_%d', 100.00)",
				d, id, last, id))
		}
	}

	// Verify index has entries.
	idxTreeKey := td.IndexFile(&td.Indexes[0])
	idxCount := 0
	env.engine.ScanRaw(idxTreeKey, []byte{0x00}, []byte{0xFF}, func(key, value []byte) bool {
		idxCount++
		return true
	})
	t.Logf("Index entries: %d (expected 30)", idxCount)
	if idxCount != 30 {
		t.Fatalf("expected 30 index entries, got %d", idxCount)
	}

	// The exact Payment c_last query (simulated placeholder replacement).
	query := "SELECT c_id, c_first FROM bmsql_customer WHERE c_w_id = 1 AND c_d_id = 5 AND c_last = 'BAR' ORDER BY c_first"
	t.Logf("Query: %s", query)

	rs, err := env.exec.Execute(query)
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	t.Logf("Result rows: %d, data: %v", len(rows.Rows), rows.Rows)

	// District 5 has c_id=13,14,15. All BAR. So 3 rows.
	if len(rows.Rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows.Rows))
	}

	// Verify ordered by c_first.
	for i, row := range rows.Rows {
		t.Logf("  row %d: c_id=%v c_first=%v", i, row[0], row[1])
	}
}

