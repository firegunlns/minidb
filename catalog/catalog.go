package catalog

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sync"

	"lns.com/minidb/bptree"
	"lns.com/minidb/storage"
)

const (
	catalogOrder    = 64
	catalogCache    = 256
	dbTreeFile      = "__catalog_dbs.db"
	tblTreeFile     = "__catalog_tables.db"
	autoIncTreeFile = "__catalog_autoinc.db"
)

type Catalog struct {
	mu      sync.RWMutex
	dataDir string
	dbTree  *bptree.PersistentBPTree
	tblTree *bptree.PersistentBPTree
	incTree *bptree.PersistentBPTree
	cache   map[string]*TableDef // "db.table" -> def
}

func Open(dataDir string) (*Catalog, error) {
	c := &Catalog{
		dataDir: dataDir,
		cache:   make(map[string]*TableDef),
	}

	var err error
	c.dbTree, err = bptree.OpenPersistentBPTree(filepath.Join(dataDir, dbTreeFile), catalogOrder, catalogCache)
	if err != nil {
		return nil, err
	}
	c.tblTree, err = bptree.OpenPersistentBPTree(filepath.Join(dataDir, tblTreeFile), catalogOrder, catalogCache)
	if err != nil {
		return nil, err
	}
	c.incTree, err = bptree.OpenPersistentBPTree(filepath.Join(dataDir, autoIncTreeFile), catalogOrder, catalogCache)
	if err != nil {
		return nil, err
	}

	// Load all tables into cache.
	c.loadCache()

	return c, nil
}

func (c *Catalog) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dbTree.Close()
	c.tblTree.Close()
	c.incTree.Close()
}

func (c *Catalog) loadCache() {
	// Scan all table entries to populate cache.
	start := []byte{0x00}
	end := []byte{0xFF}
	kvs := c.tblTree.RangeScan(start, end)
	for _, kv := range kvs {
		key := string(kv.Key)
		// Extract db and table name from key.
		idx := 0
		for i, b := range key {
			if b == 0 {
				idx = i
				break
			}
		}
		db := key[:idx]
		table := key[idx+1:]
		td := decodeTableDef(kv.Value)
		td.Database = db
		td.Name = table
		c.cache[db+"."+table] = td
	}
}

// --- Database operations ---

func (c *Catalog) CreateDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := []byte(name)
	if _, found := c.dbTree.Find(key); found {
		return fmt.Errorf("database %q already exists", name)
	}
	return c.dbTree.Insert(key, []byte(name))
}

func (c *Catalog) DropDatabase(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Remove all tables in this database.
	var toDelete []string
	for k, td := range c.cache {
		if td.Database == name {
			toDelete = append(toDelete, k)
		}
	}
	for _, k := range toDelete {
		td := c.cache[k]
		prefix := tableKeyPrefix(td.Database, td.Name)
		c.tblTree.Delete(prefix)
		delete(c.cache, k)
	}
	c.dbTree.Delete([]byte(name))
	return nil
}

func (c *Catalog) ListDatabases() ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var dbs []string
	kvs := c.dbTree.RangeScan([]byte{0x00}, []byte{0xFF})
	for _, kv := range kvs {
		dbs = append(dbs, string(kv.Key))
	}
	return dbs, nil
}

// --- Table operations ---

func tableKeyPrefix(db, table string) []byte {
	return []byte(db + "\x00" + table)
}

func (c *Catalog) CreateTable(td *TableDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := tableKeyPrefix(td.Database, td.Name)
	if _, found := c.tblTree.Find(key); found {
		return fmt.Errorf("table %s.%s already exists", td.Database, td.Name)
	}
	data := encodeTableDef(td)
	if err := c.tblTree.Insert(key, data); err != nil {
		return err
	}
	c.cache[td.Database+"."+td.Name] = td
	return nil
}

func (c *Catalog) GetTable(db, name string) (*TableDef, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if td, ok := c.cache[db+"."+name]; ok {
		return td, nil
	}
	return nil, fmt.Errorf("table %s.%s not found", db, name)
}

func (c *Catalog) ListTables(db string) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var tables []string
	for _, td := range c.cache {
		if td.Database == db {
			tables = append(tables, td.Name)
		}
	}
	return tables, nil
}

func (c *Catalog) DropTable(db, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := tableKeyPrefix(db, name)
	c.tblTree.Delete(key)
	delete(c.cache, db+"."+name)
	c.tblTree.Sync()
	return nil
}

func (c *Catalog) UpdateTable(db, name string, td *TableDef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := tableKeyPrefix(db, name)
	data := encodeTableDef(td)
	if err := c.tblTree.Insert(key, data); err != nil {
		return err
	}
	c.cache[db+"."+name] = td
	c.tblTree.Sync()
	return nil
}

// --- Auto-increment ---

func (c *Catalog) NextAutoInc(db, table, col string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := []byte("autoinc\x00" + db + "\x00" + table + "\x00" + col)
	var cur int64
	if val, found := c.incTree.Find(key); found {
		cur = int64(binary.BigEndian.Uint64(val))
	}
	cur++
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(cur))
	if err := c.incTree.Insert(key, buf); err != nil {
		return 0, err
	}
	c.incTree.Sync()
	return cur, nil
}

// --- Serialization ---

func encodeTableDef(td *TableDef) []byte {
	// Simple encoding: use a flat binary format.
	// [2B numCols][columns...][2B numIndexes][indexes...][2B numPKCols][pkCols...][2B numFKs][fk...]
	size := 2
	for _, col := range td.Columns {
		size += 2 + len(col.Name) + 1 + 2 + 1 + 1 + 1 + 1 // name + type + length + prec + scale + nullable + autoinc
	}
	size += 2
	for _, idx := range td.Indexes {
		size += 2 + len(idx.Name) + 1 + 1 + 2
		for _, cn := range idx.Columns {
			size += 2 + len(cn)
		}
	}
	size += 2 + len(td.PKCols)*2
	size += 2
	for _, fk := range td.ForeignKeys {
		size += 2 + len(fk.Name)
		size += 2 + len(fk.RefTable)
		size += 2 + len(fk.Columns)*2
		size += 2 + len(fk.RefColumns)*2
	}

	buf := make([]byte, size)
	off := 0

	binary.BigEndian.PutUint16(buf[off:], uint16(len(td.Columns)))
	off += 2

	for _, col := range td.Columns {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(col.Name)))
		off += 2
		copy(buf[off:], col.Name)
		off += len(col.Name)
		buf[off] = byte(col.Type)
		off++
		binary.BigEndian.PutUint16(buf[off:], uint16(col.Length))
		off += 2
		buf[off] = byte(col.Precision)
		off++
		buf[off] = byte(col.Scale)
		off++
		if col.Nullable {
			buf[off] = 1
		}
		off++
		if col.AutoInc {
			buf[off] = 1
		}
		off++
	}

	binary.BigEndian.PutUint16(buf[off:], uint16(len(td.Indexes)))
	off += 2
	for _, idx := range td.Indexes {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(idx.Name)))
		off += 2
		copy(buf[off:], idx.Name)
		off += len(idx.Name)
		if idx.Unique {
			buf[off] = 1
		}
		off++
		if idx.Primary {
			buf[off] = 1
		}
		off++
		binary.BigEndian.PutUint16(buf[off:], uint16(len(idx.Columns)))
		off += 2
		for _, cn := range idx.Columns {
			binary.BigEndian.PutUint16(buf[off:], uint16(len(cn)))
			off += 2
			copy(buf[off:], cn)
			off += len(cn)
		}
	}

	binary.BigEndian.PutUint16(buf[off:], uint16(len(td.PKCols)))
	off += 2
	for _, pk := range td.PKCols {
		binary.BigEndian.PutUint16(buf[off:], uint16(pk))
		off += 2
	}

	binary.BigEndian.PutUint16(buf[off:], uint16(len(td.ForeignKeys)))
	off += 2
	for _, fk := range td.ForeignKeys {
		binary.BigEndian.PutUint16(buf[off:], uint16(len(fk.Name)))
		off += 2
		copy(buf[off:], fk.Name)
		off += len(fk.Name)
		binary.BigEndian.PutUint16(buf[off:], uint16(len(fk.RefTable)))
		off += 2
		copy(buf[off:], fk.RefTable)
		off += len(fk.RefTable)
		binary.BigEndian.PutUint16(buf[off:], uint16(len(fk.Columns)))
		off += 2
		for _, c := range fk.Columns {
			binary.BigEndian.PutUint16(buf[off:], uint16(c))
			off += 2
		}
		binary.BigEndian.PutUint16(buf[off:], uint16(len(fk.RefColumns)))
		off += 2
		for _, c := range fk.RefColumns {
			binary.BigEndian.PutUint16(buf[off:], uint16(c))
			off += 2
		}
	}

	return buf[:off]
}

func decodeTableDef(data []byte) *TableDef {
	off := 0
	numCols := int(binary.BigEndian.Uint16(data[off:]))
	off += 2

	cols := make([]storage.ColumnDef, numCols)
	for i := range cols {
		nameLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		cols[i].Name = string(data[off : off+nameLen])
		off += nameLen
		cols[i].Type = storage.ColumnType(data[off])
		off++
		cols[i].Length = int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		cols[i].Precision = int(data[off])
		off++
		cols[i].Scale = int(data[off])
		off++
		cols[i].Nullable = data[off] == 1
		off++
		cols[i].AutoInc = data[off] == 1
		off++
	}

	numIdx := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	indexes := make([]IndexDef, numIdx)
	for i := range indexes {
		nameLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		indexes[i].Name = string(data[off : off+nameLen])
		off += nameLen
		indexes[i].Unique = data[off] == 1
		off++
		indexes[i].Primary = data[off] == 1
		off++
		numIdxCols := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		indexes[i].Columns = make([]string, numIdxCols)
		for j := range indexes[i].Columns {
			cnLen := int(binary.BigEndian.Uint16(data[off:]))
			off += 2
			indexes[i].Columns[j] = string(data[off : off+cnLen])
			off += cnLen
		}
	}

	numPK := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	pkCols := make([]int, numPK)
	for i := range pkCols {
		pkCols[i] = int(binary.BigEndian.Uint16(data[off:]))
		off += 2
	}

	numFK := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	fks := make([]ForeignKeyDef, numFK)
	for i := range fks {
		nameLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		fks[i].Name = string(data[off : off+nameLen])
		off += nameLen
		refTableLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		fks[i].RefTable = string(data[off : off+refTableLen])
		off += refTableLen
		numCols := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		fks[i].Columns = make([]int, numCols)
		for j := range fks[i].Columns {
			fks[i].Columns[j] = int(binary.BigEndian.Uint16(data[off:]))
			off += 2
		}
		numRefCols := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		fks[i].RefColumns = make([]int, numRefCols)
		for j := range fks[i].RefColumns {
			fks[i].RefColumns[j] = int(binary.BigEndian.Uint16(data[off:]))
			off += 2
		}
	}

	return &TableDef{
		Columns:     cols,
		Indexes:     indexes,
		PKCols:      pkCols,
		ForeignKeys: fks,
	}
}
