package sql

import (
	"testing"

	"lns.com/minidb/catalog"
	"lns.com/minidb/storage"
	"lns.com/minidb/txn"
)

func newTPCCTestEnv(t *testing.T) (*Executor, func()) {
	t.Helper()
	dir := t.TempDir()
	e, _ := storage.OpenEngine(dir, 64, 256)
	ts := txn.NewTimestampOracle()
	mgr := txn.NewManager(e, ts)
	cat, _ := catalog.Open(dir)
	exec := NewExecutor(e, mgr, cat, "")

	return exec, func() {
		cat.Close()
		e.Close()
	}
}

func TestTPCCCreateSchema(t *testing.T) {
	exec, cleanup := newTPCCTestEnv(t)
	defer cleanup()

	// Create TPC-C database.
	_, err := exec.Execute("CREATE DATABASE tpcc")
	if err != nil {
		t.Fatal(err)
	}
	_, err = exec.Execute("USE tpcc")
	if err != nil {
		t.Fatal(err)
	}

	// Create all 9 TPC-C tables.
	tables := []string{
		`CREATE TABLE warehouse (
			w_id INT NOT NULL PRIMARY KEY,
			w_name VARCHAR(10),
			w_street_1 VARCHAR(20),
			w_street_2 VARCHAR(20),
			w_city VARCHAR(20),
			w_state VARCHAR(2),
			w_zip VARCHAR(9),
			w_tax DECIMAL(4, 4),
			w_ytd DECIMAL(12, 2)
		)`,
		`CREATE TABLE district (
			d_id INT NOT NULL,
			d_w_id INT NOT NULL,
			d_name VARCHAR(10),
			d_street_1 VARCHAR(20),
			d_street_2 VARCHAR(20),
			d_city VARCHAR(20),
			d_state VARCHAR(2),
			d_zip VARCHAR(9),
			d_tax DECIMAL(4, 4),
			d_ytd DECIMAL(12, 2),
			d_next_o_id INT,
			PRIMARY KEY (d_w_id, d_id)
		)`,
		`CREATE TABLE customer (
			c_id INT NOT NULL,
			c_d_id INT NOT NULL,
			c_w_id INT NOT NULL,
			c_first VARCHAR(16),
			c_middle VARCHAR(2),
			c_last VARCHAR(16),
			c_street_1 VARCHAR(20),
			c_street_2 VARCHAR(20),
			c_city VARCHAR(20),
			c_state VARCHAR(2),
			c_zip VARCHAR(9),
			c_phone VARCHAR(16),
			c_since VARCHAR(30),
			c_credit VARCHAR(2),
			c_credit_lim DECIMAL(12, 2),
			c_discount DECIMAL(4, 4),
			c_balance DECIMAL(12, 2),
			c_ytd_payment DECIMAL(12, 2),
			c_payment_cnt INT,
			c_delivery_cnt INT,
			c_data VARCHAR(500),
			PRIMARY KEY (c_w_id, c_d_id, c_id)
		)`,
		`CREATE TABLE history (
			h_c_id INT,
			h_c_d_id INT,
			h_c_w_id INT,
			h_d_id INT,
			h_w_id INT,
			h_date VARCHAR(30),
			h_amount DECIMAL(6, 2),
			h_data VARCHAR(24)
		)`,
		`CREATE TABLE orders (
			o_id INT NOT NULL,
			o_d_id INT NOT NULL,
			o_w_id INT NOT NULL,
			o_c_id INT,
			o_entry_d VARCHAR(30),
			o_carrier_id INT,
			o_ol_cnt INT,
			o_all_local INT,
			PRIMARY KEY (o_w_id, o_d_id, o_id)
		)`,
		`CREATE TABLE new_order (
			no_o_id INT NOT NULL,
			no_d_id INT NOT NULL,
			no_w_id INT NOT NULL,
			PRIMARY KEY (no_w_id, no_d_id, no_o_id)
		)`,
		`CREATE TABLE order_line (
			ol_o_id INT NOT NULL,
			ol_d_id INT NOT NULL,
			ol_w_id INT NOT NULL,
			ol_number INT NOT NULL,
			ol_i_id INT,
			ol_supply_w_id INT,
			ol_delivery_d VARCHAR(30),
			ol_quantity INT,
			ol_amount DECIMAL(6, 2),
			ol_dist_info VARCHAR(24),
			PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
		)`,
		`CREATE TABLE item (
			i_id INT NOT NULL PRIMARY KEY,
			i_name VARCHAR(24),
			i_price DECIMAL(5, 2),
			i_data VARCHAR(50),
			i_im_id INT
		)`,
		`CREATE TABLE stock (
			s_i_id INT NOT NULL,
			s_w_id INT NOT NULL,
			s_quantity INT,
			s_dist_01 VARCHAR(24),
			s_dist_02 VARCHAR(24),
			s_dist_03 VARCHAR(24),
			s_dist_04 VARCHAR(24),
			s_dist_05 VARCHAR(24),
			s_dist_06 VARCHAR(24),
			s_dist_07 VARCHAR(24),
			s_dist_08 VARCHAR(24),
			s_dist_09 VARCHAR(24),
			s_dist_10 VARCHAR(24),
			s_ytd INT,
			s_order_cnt INT,
			s_remote_cnt INT,
			s_data VARCHAR(50),
			PRIMARY KEY (s_w_id, s_i_id)
		)`,
	}

	for _, ddl := range tables {
		if _, err := exec.Execute(ddl); err != nil {
			t.Fatalf("create table failed: %v\nSQL: %s", err, ddl)
		}
	}

	// Verify all tables exist.
	cat := exec.cat
	tableList, err := cat.ListTables("tpcc")
	if err != nil {
		t.Fatal(err)
	}
	if len(tableList) != 9 {
		t.Errorf("expected 9 tables, got %d: %v", len(tableList), tableList)
	}
}

func TestTPCCInsertWarehouse(t *testing.T) {
	exec, cleanup := newTPCCTestEnv(t)
	defer cleanup()

	exec.Execute("CREATE DATABASE tpcc")
	exec.Execute("USE tpcc")
	exec.Execute(`CREATE TABLE warehouse (
		w_id INT NOT NULL PRIMARY KEY,
		w_name VARCHAR(10),
		w_tax DECIMAL(4, 4),
		w_ytd DECIMAL(12, 2)
	)`)

	// Insert a warehouse.
	_, err := exec.Execute("INSERT INTO warehouse (w_id, w_name, w_tax, w_ytd) VALUES (1, 'W1', '0.1234', '100000.00')")
	if err != nil {
		t.Fatal(err)
	}

	// Read it back.
	rs, err := exec.Execute("SELECT w_id, w_name, w_tax, w_ytd FROM warehouse WHERE w_id = 1")
	if err != nil {
		t.Fatal(err)
	}
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows.Rows))
	}
	if rows.Rows[0][0].(int32) != 1 {
		t.Errorf("expected w_id=1, got %v", rows.Rows[0][0])
	}
}

func TestTPCCNewOrderTxn(t *testing.T) {
	exec, cleanup := newTPCCTestEnv(t)
	defer cleanup()

	exec.Execute("CREATE DATABASE tpcc")
	exec.Execute("USE tpcc")

	// Create minimal tables for New-Order.
	exec.Execute(`CREATE TABLE warehouse (w_id INT NOT NULL PRIMARY KEY, w_tax DECIMAL(4, 4), w_ytd DECIMAL(12, 2))`)
	exec.Execute(`CREATE TABLE district (d_id INT NOT NULL, d_w_id INT NOT NULL, d_tax DECIMAL(4, 4), d_next_o_id INT, PRIMARY KEY (d_w_id, d_id))`)
	exec.Execute(`CREATE TABLE customer (c_id INT NOT NULL, c_d_id INT NOT NULL, c_w_id INT NOT NULL, c_discount DECIMAL(4, 4), c_last VARCHAR(16), PRIMARY KEY (c_w_id, c_d_id, c_id))`)
	exec.Execute(`CREATE TABLE item (i_id INT NOT NULL PRIMARY KEY, i_name VARCHAR(24), i_price DECIMAL(5, 2))`)
	exec.Execute(`CREATE TABLE stock (s_i_id INT NOT NULL, s_w_id INT NOT NULL, s_quantity INT, s_data VARCHAR(50), PRIMARY KEY (s_w_id, s_i_id))`)
	exec.Execute(`CREATE TABLE orders (o_id INT NOT NULL, o_d_id INT NOT NULL, o_w_id INT NOT NULL, o_c_id INT, o_ol_cnt INT, PRIMARY KEY (o_w_id, o_d_id, o_id))`)
	exec.Execute(`CREATE TABLE new_order (no_o_id INT NOT NULL, no_d_id INT NOT NULL, no_w_id INT NOT NULL, PRIMARY KEY (no_w_id, no_d_id, no_o_id))`)
	exec.Execute(`CREATE TABLE order_line (ol_o_id INT NOT NULL, ol_d_id INT NOT NULL, ol_w_id INT NOT NULL, ol_number INT NOT NULL, ol_i_id INT, ol_quantity INT, ol_amount DECIMAL(6, 2), PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number))`)

	// Load data.
	exec.Execute("INSERT INTO warehouse (w_id, w_tax, w_ytd) VALUES (1, '0.1000', '100000.00')")
	exec.Execute("INSERT INTO district (d_id, d_w_id, d_tax, d_next_o_id) VALUES (1, 1, '0.0500', 100)")
	exec.Execute("INSERT INTO customer (c_id, c_d_id, c_w_id, c_discount, c_last) VALUES (1, 1, 1, '0.0500', 'SMITH')")
	exec.Execute("INSERT INTO item (i_id, i_name, i_price) VALUES (1, 'ITEM1', '10.00')")
	exec.Execute("INSERT INTO item (i_id, i_name, i_price) VALUES (2, 'ITEM2', '20.00')")
	exec.Execute("INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_data) VALUES (1, 1, 100, 'DATA1')")
	exec.Execute("INSERT INTO stock (s_i_id, s_w_id, s_quantity, s_data) VALUES (2, 1, 200, 'DATA2')")

	// New-Order transaction.
	exec.Execute("BEGIN")

	// Read customer.
	rs, _ := exec.Execute("SELECT c_discount, c_last FROM customer WHERE c_w_id = 1 AND c_d_id = 1 AND c_id = 1")
	rows := rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatal("customer not found")
	}

	// Read warehouse tax.
	rs, _ = exec.Execute("SELECT w_tax FROM warehouse WHERE w_id = 1")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatal("warehouse not found")
	}

	// Read district and update next_o_id.
	rs, _ = exec.Execute("SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1")
	rows = rs.(*SelectResult)
	if len(rows.Rows) != 1 {
		t.Fatal("district not found")
	}
	nextOID := rows.Rows[0][1].(int32)

	exec.Execute("UPDATE district SET d_next_o_id = 101 WHERE d_w_id = 1 AND d_id = 1")

	// Create order.
	exec.Execute("INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) VALUES (100, 1, 1, 1, 2)")
	exec.Execute("INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES (100, 1, 1)")

	// Create order lines and update stock.
	exec.Execute("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_quantity, ol_amount) VALUES (100, 1, 1, 1, 1, 5, '50.00')")
	exec.Execute("INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_quantity, ol_amount) VALUES (100, 1, 1, 2, 2, 3, '60.00')")

	exec.Execute("UPDATE stock SET s_quantity = 95 WHERE s_w_id = 1 AND s_i_id = 1")
	exec.Execute("UPDATE stock SET s_quantity = 197 WHERE s_w_id = 1 AND s_i_id = 2")

	exec.Execute("COMMIT")

	// Verify.
	rs, _ = exec.Execute("SELECT d_next_o_id FROM district WHERE d_w_id = 1 AND d_id = 1")
	rows = rs.(*SelectResult)
	if rows.Rows[0][0].(int32) != 101 {
		t.Errorf("expected d_next_o_id=101, got %v", rows.Rows[0][0])
	}

	rs, _ = exec.Execute("SELECT s_quantity FROM stock WHERE s_w_id = 1 AND s_i_id = 1")
	rows = rs.(*SelectResult)
	if rows.Rows[0][0].(int32) != 95 {
		t.Errorf("expected s_quantity=95, got %v", rows.Rows[0][0])
	}

	_ = nextOID
}
