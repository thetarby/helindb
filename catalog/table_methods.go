package catalog

import (
	"helin/catalog/db_types"
	"helin/common"
	"helin/concurrency"
	"helin/disk/structures"
)

func (tbl *TableInfo) InsertTuple(values []*db_types.Value, txn concurrency.Transaction) (*structures.Rid, error) {
	tuple, err := NewTupleWithSchema(values, tbl.Schema)
	if err != nil {
		return nil, err
	}

	rid, err := tbl.Heap.InsertTuple(*tuple.GetRow(), txn)
	if err != nil {
		return nil, err
	}

	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		indexedCol := index.IndexedColIdx
		key := tuple.GetValue(tbl.Schema, indexedCol)
		index.Index.Insert(key, rid) // TODO: txn should pass down
	}

	return &rid, nil
}

func (tbl *TableInfo) InsertTuple2(values []*db_types.Value, txn concurrency.Transaction) (*structures.Rid, error) {
	tuple, err := NewTupleWithSchema(values, tbl.Schema)
	if err != nil {
		return nil, err
	}

	rid, err := tbl.Heap.InsertTuple(*tuple.GetRow(), txn)
	if err != nil {
		return nil, err
	}
	tuple.Rid = rid

	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		index.InsertViaTuple(tuple, rid)
	}

	return &rid, nil
}

func (tbl *TableInfo) DeleteTuple(rid structures.Rid, txn concurrency.Transaction) error {
	oldTuple := Tuple{
		Row: structures.Row{},
	}
	if err := tbl.Heap.ReadTuple(rid, oldTuple.GetRow(), txn); err != nil {
		return err
	}

	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		indexedCol := index.IndexedColIdx
		key := oldTuple.GetValue(tbl.Schema, indexedCol)
		index.Index.Delete(key)
	}

	if err := tbl.Heap.HardDeleteTuple(rid, txn); err != nil {
		return err
	}

	return nil
}

func (tbl *TableInfo) UpdateTuple(rid structures.Rid, values []*db_types.Value, txn concurrency.Transaction) error {
	oldTuple := Tuple{
		Row: structures.Row{},
	}
	if err := tbl.Heap.ReadTuple(rid, oldTuple.GetRow(), txn); err != nil {
		return err
	}

	newTuple, err := NewTupleWithSchema(values, tbl.Schema)
	if err != nil {
		return err
	}

	indexes := tbl.GetIndexes()

	// first try in-place update and if it succeeds update indexes and return directly
	if err := tbl.Heap.UpdateTuple(newTuple.Row, rid, txn); err == nil {
		for _, index := range indexes {
			indexedCol := index.IndexedColIdx
			key := oldTuple.GetValue(tbl.Schema, indexedCol)
			index.Index.Delete(key) // TODO: can this return false? should not
			index.Index.Insert(newTuple.GetValue(tbl.Schema, indexedCol), newTuple.GetRid())
		}
		return nil
	}

	// else try delete-insert
	if err := tbl.DeleteTuple(rid, txn); err != nil {
		return err
	}
	newRid ,err := tbl.InsertTuple(values, txn)
	if err != nil {
		return err
	}

	for _, index := range indexes {
		indexedCol := index.IndexedColIdx
		key := oldTuple.GetValue(tbl.Schema, indexedCol)
		index.Index.Delete(key) // TODO: can this return false? should not
		index.Index.Insert(newTuple.GetValue(tbl.Schema, indexedCol), newRid)
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

func (index *IndexInfo) GetTable() *TableInfo {
	return index.catalog.GetTable(index.TableName)
}

func (index *IndexInfo) InsertViaTuple(tuple *Tuple, val interface{}){
	tbl := index.GetTable()
	vals := make([]*db_types.Value, 0)
	for _, idx := range index.ColumnIndexes {
		val := tuple.GetValue(tbl.Schema, idx)
		vals = append(vals, val)
	}

	if !index.IsUnique {
		vals = append(vals, db_types.NewValue(int32(tuple.Rid.PageId)), db_types.NewValue(int32(tuple.Rid.SlotIdx)))
	}

	t, err := NewTupleWithSchema(vals, index.Schema)
	common.PanicIfErr(err)

	tupleKey := TupleKey{
		Schema: index.Schema,
		Tuple:  *t,
	}

	index.Index.Insert(&tupleKey, val) // TODO: no need to store a value if rid is in key itself
}
