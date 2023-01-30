package catalog

import (
	"helin/catalog/db_types"
	"helin/disk/structures"
	"helin/transaction"
)

func (tbl *TableInfo) InsertTupleViaValues(values []*db_types.Value, txn transaction.Transaction) (*structures.Rid, error) {
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
		index.InsertTupleKey(tuple, rid)
	}

	return &rid, nil
}

func (tbl *TableInfo) InsertTuple(tuple *Tuple, txn transaction.Transaction) (*structures.Rid, error) {
	rid, err := tbl.Heap.InsertTuple(*tuple.GetRow(), txn)
	if err != nil {
		return nil, err
	}
	tuple.Rid = rid

	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		index.InsertTupleKey(tuple, rid)
	}

	return &rid, nil
}

func (tbl *TableInfo) DeleteTuple(rid structures.Rid, txn transaction.Transaction) error {
	oldTuple := Tuple{
		Row: structures.Row{},
	}
	if err := tbl.Heap.ReadTuple(rid, oldTuple.GetRow(), txn); err != nil {
		return err
	}

	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		index.DeleteTupleKey(&oldTuple)
	}

	if err := tbl.Heap.HardDeleteTuple(rid, txn); err != nil {
		return err
	}

	return nil
}

func (tbl *TableInfo) UpdateTuple(rid structures.Rid, values []*db_types.Value, txn transaction.Transaction) error {
	// first read old tuple from heap
	oldTuple := Tuple{
		Row: structures.Row{},
	}
	if err := tbl.Heap.ReadTuple(rid, oldTuple.GetRow(), txn); err != nil {
		return err
	}

	// convert values list to tuple
	newTuple, err := NewTupleWithSchema(values, tbl.Schema)
	if err != nil {
		return err
	}

	// first try in-place update and if it succeeds update indexes and return directly
	if err := tbl.Heap.UpdateTuple(newTuple.Row, rid, txn); err == nil {
		indexes := tbl.GetIndexes()
		for _, index := range indexes {
			index.UpdateTupleKey(&oldTuple, newTuple.GetRid()) // TODO: can this return false? should not
		}
		return nil
	}

	// else try delete-insert
	if err := tbl.DeleteTuple(rid, txn); err != nil {
		return err
	}
	newRid, err := tbl.InsertTupleViaValues(values, txn)
	if err != nil {
		return err
	}

	// update all indexes as well
	indexes := tbl.GetIndexes()
	for _, index := range indexes {
		index.UpdateTupleKey(&oldTuple, newRid) // TODO: can this return false? should not
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

func (index *IndexInfo) InsertTupleKey(tuple *Tuple, val any) {
	vals := make([]*db_types.Value, 0)
	for _, idx := range index.ColumnIndexes {
		val := tuple.GetValue(index.GetTable().Schema, idx)
		vals = append(vals, val)
	}

	if !index.IsUnique {
		vals = append(vals, db_types.NewValue(int32(tuple.Rid.PageId)), db_types.NewValue(int32(tuple.Rid.SlotIdx)))
	}

	tk := NewTupleKey(index.Schema, vals...)
	index.Index.Insert(transaction.TxnTODO(), &tk, val) // TODO: no need to store a value if rid is in key itself
}

func (index *IndexInfo) DeleteTupleKey(tuple *Tuple) {
	// get columns to construct tuple key
	vals := make([]*db_types.Value, 0)
	for _, idx := range index.ColumnIndexes {
		val := tuple.GetValue(index.GetTable().Schema, idx)
		vals = append(vals, val)
	}

	if !index.IsUnique {
		vals = append(vals, db_types.NewValue(int32(tuple.Rid.PageId)), db_types.NewValue(int32(tuple.Rid.SlotIdx)))
	}

	tk := NewTupleKey(index.Schema, vals...)
	index.Index.Delete(nil, &tk)
}

func (index *IndexInfo) UpdateTupleKey(tuple *Tuple, val any) {
	// get columns to construct tuple key
	vals := make([]*db_types.Value, 0)
	for _, idx := range index.ColumnIndexes {
		val := tuple.GetValue(index.GetTable().Schema, idx)
		vals = append(vals, val)
	}

	if !index.IsUnique {
		vals = append(vals, db_types.NewValue(int32(tuple.Rid.PageId)), db_types.NewValue(int32(tuple.Rid.SlotIdx)))
	}

	tk := NewTupleKey(index.Schema, vals...)
	index.Index.InsertOrReplace(nil, &tk, val)
}
