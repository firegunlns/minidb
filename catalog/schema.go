// Package catalog 提供数据库元数据目录功能
package catalog

import "lns.com/minidb/storage"

// DatabaseDef 数据库定义
type DatabaseDef struct {
	Name string // 数据库名称
}

// TableStats 表统计信息
type TableStats struct {
	RowCount   int64         // 总行数
	UpdateTime int64         // unix timestamp
	ColStats   []ColumnStats // 每列统计信息
}

// ColumnStats 列统计信息
type ColumnStats struct {
	Name    string // 列名
	NDV     int64  // 不同值的数量
	NullCnt int64  // NULL 值数量
	MinVal  any    // 最小值 (int64, float64, or string)
	MaxVal  any    // 最大值
	AvgLen  int64  // varchar 列平均字节长度
}

// TableDef 表定义
// 存储表的完整元数据信息
type TableDef struct {
	Database    string              // 所属数据库
	Name        string              // 表名
	Columns     []storage.ColumnDef // 列定义
	PKCols      []int               // 主键列在Columns中的索引
	Indexes     []IndexDef          // 索引定义
	ForeignKeys []ForeignKeyDef     // 外键定义
	Stats       *TableStats         // 统计信息 (nil until ANALYZE TABLE)
}

// ForeignKeyDef 外键定义
type ForeignKeyDef struct {
	Name       string // 外键名称
	Columns    []int  // 本表中的列索引
	RefTable   string // 引用的表名
	RefColumns []int  // 被引用表中的列索引
}

func (td *TableDef) PrimaryKeyColumns() []storage.ColumnDef {
	cols := make([]storage.ColumnDef, len(td.PKCols))
	for i, idx := range td.PKCols {
		cols[i] = td.Columns[idx]
	}
	return cols
}

func (td *TableDef) ColumnIndex(name string) int {
	for i, c := range td.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

type IndexDef struct {
	Name    string
	Columns []string
	Unique  bool
	Primary bool
}

func (td *TableDef) DataFile() string {
	return td.Database + "__" + td.Name + ".db"
}

func (td *TableDef) IndexFile(idx *IndexDef) string {
	return td.Database + "__" + td.Name + "__idx__" + idx.Name + ".db"
}
