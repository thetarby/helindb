package catalog

import (
	"helin/catalog/db_types"
	"helin/concurrency"
)

func (tbl *TableInfo) InsertTuple(values []*db_types.Value, txn concurrency.Transaction) error {
	tuple, err := NewTupleWithSchema(values, tbl.Schema)
	if err != nil {
		return err
	}

	rid, err := tbl.Heap.InsertTuple(*tuple.GetRow(), txn)
	if err != nil {
		return err
	}

	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		indexedCol := index.IndexedColIdx
		key := tuple.GetValue(tbl.Schema, indexedCol)
		index.Index.Insert(key, rid) // TODO: txn should pass down
	}

	return nil
}

func (tbl *TableInfo) GetIndexes() []*IndexInfo {
	res := make([]*IndexInfo, 0)
	for _, val := range tbl.catalog.indexNames[tbl.Name] {
		res = append(res, tbl.catalog.indexes[val])
	}

	return res
}
