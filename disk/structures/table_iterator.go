package structures

import (
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
)

type Iterator interface {
	Next() interface{}
}

type TableIterator struct {
	txn  transaction.Transaction
	rid  Rid
	heap *TableHeap
}

func (it *TableIterator) Next() *Row {
	// TODO: get pool from somewhere else
	pool := it.heap.Pool
	dest := Row{}

	currPage, err := pool.GetPage(it.rid.PageId)
	common.PanicIfErr(err)
	sp := pages.AsHeapPage(currPage)

	nextIdx, err := sp.GetNextIdx(int(it.rid.SlotIdx))
	if err != nil {
		for {
			nextPageID := sp.GetHeader().NextPageID
			if nextPageID == 0 {
				// we come to the end of heap
				return nil
			}

			currPage, err = pool.GetPage(nextPageID)
			common.PanicIfErr(err)
			sp = pages.AsHeapPage(currPage)
			nextIdx, err = sp.GetNextIdx(-1)
			if err != nil {
				continue
			}
			break
		}
	}

	nextRid := Rid{
		PageId:  sp.GetPageId(),
		SlotIdx: int16(nextIdx),
	}
	if err := it.heap.ReadTuple(nextRid, &dest, it.txn); err != nil {
		panic(err)
	}

	it.rid = nextRid
	return &dest
}

func NewTableIterator(txn transaction.Transaction, heap *TableHeap) *TableIterator {
	return &TableIterator{
		txn: txn,
		rid: Rid{
			PageId:  heap.FirstPageID,
			SlotIdx: -1,
		},
		heap: heap,
	}
}
