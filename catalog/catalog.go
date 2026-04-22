// Package catalog 提供数据库元数据目录功能
// 存储和管理数据库、表、列、索引等元数据
package catalog

import (
	"encoding/binary"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"

	"lns.com/minidb/bptree"
	"lns.com/minidb/storage"
)

const (
	catalogOrder    = 64                     // 目录B+树的阶
	catalogCache    = 256                    // 目录缓存大小
	dbTreeFile      = "__catalog_dbs.db"     // 数据库元数据树文件
	tblTreeFile     = "__catalog_tables.db"  // 表元数据树文件
	autoIncTreeFile = "__catalog_autoinc.db" // 自增序列树文件
)

// Catalog 数据库目录
// 存储所有数据库和表的元数据
type Catalog struct {
	mu       sync.RWMutex            // 保护 dbTree/tblTree/cache
	incMu    sync.Mutex              // 独立锁，保护自增序列
	dataDir  string
	dbTree   *bptree.PersistentBPTree // 数据库树
	tblTree  *bptree.PersistentBPTree // 表树
	incTree  *bptree.PersistentBPTree // 自增序列树（仅 Close 时持久化）
	cache    map[string]*TableDef     // 表定义缓存 "db.table" -> TableDef
	incCache map[string]int64         // 自增序列内存缓存 "db\x00table\x00col" -> 当前值
}

func Open(dataDir string) (*Catalog, error) {
	c := &Catalog{
		dataDir:  dataDir,
		cache:    make(map[string]*TableDef),
		incCache: make(map[string]int64),
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

	// Load all auto-inc values into memory.
	c.loadIncCache()

	return c, nil
}

func (c *Catalog) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.incMu.Lock()
	defer c.incMu.Unlock()
	c.flushIncCache()
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
	if err := c.dbTree.Insert(key, []byte(name)); err != nil {
		return err
	}
	return c.dbTree.Sync()
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
	return c.tblTree.Sync()
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

// autoIncKey returns the in-memory cache key for an auto-inc counter.
func autoIncKey(db, table, col string) string {
	return db + "\x00" + table + "\x00" + col
}

func (c *Catalog) loadIncCache() {
	kvs := c.incTree.RangeScan([]byte{0x00}, []byte{0xFF})
	for _, kv := range kvs {
		// Disk key: "autoinc\x00" + db + "\x00" + table + "\x00" + col
		parts := strings.SplitN(string(kv.Key), "\x00", 4)
		if len(parts) == 4 {
			memKey := parts[1] + "\x00" + parts[2] + "\x00" + parts[3]
			c.incCache[memKey] = int64(binary.BigEndian.Uint64(kv.Value))
		}
	}
}

func (c *Catalog) NextAutoInc(db, table, col string) (int64, error) {
	key := autoIncKey(db, table, col)
	c.incMu.Lock()
	c.incCache[key]++
	val := c.incCache[key]
	c.incMu.Unlock()
	return val, nil
}

// flushIncCache writes all in-memory auto-inc counters to the incTree.
func (c *Catalog) flushIncCache() {
	for memKey, val := range c.incCache {
		parts := strings.SplitN(memKey, "\x00", 3)
		diskKey := []byte("autoinc\x00" + parts[0] + "\x00" + parts[1] + "\x00" + parts[2])
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(val))
		c.incTree.Insert(diskKey, buf)
	}
	c.incTree.Sync()
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

	// Stats trailer: [1B hasStats]
	if td.Stats != nil {
		buf = append(buf, 1) // hasStats = true
		buf = appendStats(buf, td.Stats)
	} else {
		buf = append(buf, 0) // hasStats = false
	}

	return buf
}

func appendStats(buf []byte, stats *TableStats) []byte {
	// [8B rowCount][8B updateTime][2B numColStats][per-column data...]
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(stats.RowCount))
	buf = append(buf, tmp[:]...)
	binary.BigEndian.PutUint64(tmp[:], uint64(stats.UpdateTime))
	buf = append(buf, tmp[:]...)
	binary.BigEndian.PutUint16(tmp[:2], uint16(len(stats.ColStats)))
	buf = append(buf, tmp[:2]...)
	for _, cs := range stats.ColStats {
		buf = appendColumnStats(buf, cs)
	}
	return buf
}

func appendColumnStats(buf []byte, cs ColumnStats) []byte {
	// [2B nameLen][name][8B ndv][8B nullCnt][1B valType][minVal bytes][1B valType][maxVal bytes][8B avgLen]
	var tmp [8]byte
	binary.BigEndian.PutUint16(tmp[:2], uint16(len(cs.Name)))
	buf = append(buf, tmp[:2]...)
	buf = append(buf, cs.Name...)
	binary.BigEndian.PutUint64(tmp[:], uint64(cs.NDV))
	buf = append(buf, tmp[:]...)
	binary.BigEndian.PutUint64(tmp[:], uint64(cs.NullCnt))
	buf = append(buf, tmp[:]...)
	buf = appendStatsValue(buf, cs.MinVal)
	buf = appendStatsValue(buf, cs.MaxVal)
	binary.BigEndian.PutUint64(tmp[:], uint64(cs.AvgLen))
	buf = append(buf, tmp[:]...)
	return buf
}

// stats value encoding: [1B valType: 0=nil, 1=int64, 2=float64, 3=string][value bytes]
func appendStatsValue(buf []byte, val any) []byte {
	if val == nil {
		buf = append(buf, 0)
		return buf
	}
	var tmp [8]byte
	switch v := val.(type) {
	case int64:
		buf = append(buf, 1)
		binary.BigEndian.PutUint64(tmp[:], uint64(v))
		buf = append(buf, tmp[:]...)
	case int32:
		buf = append(buf, 1)
		binary.BigEndian.PutUint64(tmp[:], uint64(v))
		buf = append(buf, tmp[:]...)
	case int:
		buf = append(buf, 1)
		binary.BigEndian.PutUint64(tmp[:], uint64(v))
		buf = append(buf, tmp[:]...)
	case float64:
		buf = append(buf, 2)
		binary.BigEndian.PutUint64(tmp[:], math.Float64bits(v))
		buf = append(buf, tmp[:]...)
	case string:
		buf = append(buf, 3)
		binary.BigEndian.PutUint16(tmp[:2], uint16(len(v)))
		buf = append(buf, tmp[:2]...)
		buf = append(buf, v...)
	default:
		buf = append(buf, 0)
	}
	return buf
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

	// Parse stats trailer if present (backward compatible: old format has no trailer).
	var stats *TableStats
	if off < len(data) {
		hasStats := data[off]
		off++
		if hasStats == 1 {
			stats = &TableStats{}
			stats.RowCount = int64(binary.BigEndian.Uint64(data[off:]))
			off += 8
			stats.UpdateTime = int64(binary.BigEndian.Uint64(data[off:]))
			off += 8
			numColStats := int(binary.BigEndian.Uint16(data[off:]))
			off += 2
			stats.ColStats = make([]ColumnStats, numColStats)
			for i := range stats.ColStats {
				cs, n := decodeColumnStats(data[off:])
				stats.ColStats[i] = cs
				off += n
			}
		}
	}

	return &TableDef{
		Columns:     cols,
		Indexes:     indexes,
		PKCols:      pkCols,
		ForeignKeys: fks,
		Stats:       stats,
	}
}

func decodeColumnStats(data []byte) (ColumnStats, int) {
	off := 0
	var cs ColumnStats
	nameLen := int(binary.BigEndian.Uint16(data[off:]))
	off += 2
	cs.Name = string(data[off : off+nameLen])
	off += nameLen
	cs.NDV = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	cs.NullCnt = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	minVal, n := decodeStatsValue(data[off:])
	cs.MinVal = minVal
	off += n
	maxVal, n := decodeStatsValue(data[off:])
	cs.MaxVal = maxVal
	off += n
	cs.AvgLen = int64(binary.BigEndian.Uint64(data[off:]))
	off += 8
	return cs, off
}

func decodeStatsValue(data []byte) (any, int) {
	valType := data[0]
	off := 1
	switch valType {
	case 0: // nil
		return nil, off
	case 1: // int64
		v := int64(binary.BigEndian.Uint64(data[off:]))
		return v, off + 8
	case 2: // float64
		v := math.Float64frombits(binary.BigEndian.Uint64(data[off:]))
		return v, off + 8
	case 3: // string
		strLen := int(binary.BigEndian.Uint16(data[off:]))
		off += 2
		return string(data[off : off+strLen]), off + strLen
	}
	return nil, off
}
