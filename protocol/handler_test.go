package protocol

import (
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"lns.com/minidb/catalog"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
	"lns.com/minidb/wal"
)

func startTestServer(t *testing.T) (*Server, string) {
	t.Helper()
	dir := t.TempDir()

	engine, err := storage.OpenEngine(dir, 64, 256)
	if err != nil {
		t.Fatal(err)
	}
	ts := txn.NewTimestampOracle()
	w, _ := wal.Open(dir)
	mgr := txn.NewManager(engine, ts, w)
	cat, err := catalog.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	svr, err := NewServer("127.0.0.1:0", engine, mgr, cat)
	if err != nil {
		t.Fatal(err)
	}

	go svr.Serve()
	time.Sleep(100 * time.Millisecond)

	return svr, svr.Addr().String()
}

func openDB(t *testing.T, addr string) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf("root@tcp(%s)/", addr))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	return db
}

func TestProtocolConnect(t *testing.T) {
	svr, addr := startTestServer(t)
	defer svr.Close()

	db := openDB(t, addr)
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
}

func TestProtocolCreateDBAndTable(t *testing.T) {
	svr, addr := startTestServer(t)
	defer svr.Close()

	db := openDB(t, addr)
	defer db.Close()

	if _, err := db.Exec("CREATE DATABASE testdb"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("USE testdb"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE users (id INT NOT NULL PRIMARY KEY, name VARCHAR(50))`); err != nil {
		t.Fatal(err)
	}
}

func TestProtocolInsertSelect(t *testing.T) {
	svr, addr := startTestServer(t)
	defer svr.Close()

	db := openDB(t, addr)
	defer db.Close()

	db.Exec("CREATE DATABASE testdb")
	db.Exec("USE testdb")
	db.Exec(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, val INT)`)
	db.Exec("INSERT INTO t1 (id, val) VALUES (1, 100)")

	rows, err := db.Query("SELECT id, val FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var id, val int
	if err := rows.Scan(&id, &val); err != nil {
		t.Fatal(err)
	}
	if id != 1 || val != 100 {
		t.Errorf("expected (1, 100), got (%d, %d)", id, val)
	}
}

func TestProtocolTransactionExplicit(t *testing.T) {
	svr, addr := startTestServer(t)
	defer svr.Close()

	db := openDB(t, addr)
	defer db.Close()

	db.Exec("CREATE DATABASE testdb")
	db.Exec("USE testdb")
	db.Exec(`CREATE TABLE t1 (id INT NOT NULL PRIMARY KEY, v INT)`)
	db.Exec("INSERT INTO t1 (id, v) VALUES (1, 10)")

	// Use explicit BEGIN/COMMIT instead of db.Begin().
	if _, err := db.Exec("BEGIN"); err != nil {
		t.Fatal("BEGIN:", err)
	}
	if _, err := db.Exec("UPDATE t1 SET v = 99 WHERE id = 1"); err != nil {
		t.Fatal("UPDATE:", err)
	}
	if _, err := db.Exec("COMMIT"); err != nil {
		t.Fatal("COMMIT:", err)
	}

	rows, err := db.Query("SELECT v FROM t1 WHERE id = 1")
	if err != nil {
		t.Fatal("SELECT:", err)
	}
	defer rows.Close()
	if rows.Next() {
		var v int
		rows.Scan(&v)
		if v != 99 {
			t.Errorf("expected v=99, got %d", v)
		}
	}
}
