package catalog

import "lns.com/minidb/storage"

type DatabaseDef struct {
	Name string
}

type TableDef struct {
	Database string
	Name     string
	Columns  []storage.ColumnDef
	PKCols   []int // indices into Columns that form the primary key
	Indexes  []IndexDef
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
