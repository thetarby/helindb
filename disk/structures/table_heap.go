package structures

import (
	"helin/btree"
	"helin/buffer"
	"helin/concurrency"
	"helin/disk/pages"
)

type Rid btree.SlotPointer

func NewRid(pageID, slotIdx int) Rid {
	return Rid{
		PageId:  int64(pageID),
		SlotIdx: int16(slotIdx),
	}
}

type ITableHeap interface {
	// InsertTuple Insert a tuple into the table. If the tuple is too large (>= page_size), return error.
	InsertTuple(tuple Row, txn concurrency.Transaction) (Rid, error)

	// UpdateTuple if the new tuple is too large to fit in the old page, return error (will delete and insert)
	UpdateTuple(tuple Row, rid Rid, txn concurrency.Transaction) error

	// ReadTuple if tuple does not exist at Rid returns an error
	ReadTuple(rid Rid, dest *Row, txn concurrency.Transaction) error

	// HardDeleteTuple if tuple does not exist at Rid returns an error
	HardDeleteTuple(rid Rid, txn concurrency.Transaction) error

	// Vacuum compresses the structure so that there are no gaps between pages and in pages.
	Vacuum() error
}

type TableHeap struct {
	Pool        *buffer.BufferPool
	FirstPageID int
	LastPageID  int
}

func (t *TableHeap) HardDeleteTuple(rid Rid, txn concurrency.Transaction) error {
	page, err := t.Pool.GetPage(int(rid.PageId))
	if err != nil {
		return err
	}

	heapPage := pages.AsHeapPage(page)
	heapPage.WLatch()
	defer heapPage.WUnlatch()
	defer t.Pool.Unpin(heapPage.GetPageId(), true)

	if err := heapPage.HardDelete(int(rid.SlotIdx)); err != nil {
		return err
	}

	return nil
}

func (t *TableHeap) InsertTuple(tuple Row, txn concurrency.Transaction) (Rid, error) {
	// TODO: does not set rid in row
	// TODO: unpin pages
	currPage, err := t.GetFirstPage()
	if err != nil {
		return Rid{}, err
	}

	for {
		currPage.WLatch()
		// if there is enough space in the current page insert tuple and return Rid
		if currPage.GetFreeSpace() >= (tuple.Length())+pages.SLOT_ARRAY_ENTRY_SIZE {
			idx, err := currPage.InsertTuple(tuple.GetData())
			if err != nil {
				currPage.WUnlatch()
				return Rid{}, err
			}

			currPage.WUnlatch()
			t.Pool.Unpin(currPage.GetPageId(), true)
			return NewRid(currPage.GetPageId(), idx), nil
		}

		// else get next page and try again
		if currPage.GetHeader().NextPageID == 0 {
			page, err := t.Pool.NewPage()
			if err != nil {
				return Rid{}, err
			}

			h := currPage.GetHeader()
			h.NextPageID = int64(page.GetPageId())
			currPage.SetHeader(h)

			currPage.WUnlatch()
			t.Pool.Unpin(currPage.GetPageId(), true)
			currPage = pages.InitHeapPage(page)
			continue
		}

		currPage.WUnlatch()
		// if next page id is set move on to that page
		t.Pool.Unpin(currPage.GetPageId(), false)
		raw, err := t.Pool.GetPage(int(currPage.GetHeader().NextPageID))
		if err != nil {
			return Rid{}, err
		}
		currPage = pages.AsHeapPage(raw)
	}
}

func (t *TableHeap) UpdateTuple(tuple Row, rid Rid, txn concurrency.Transaction) error {
	page, err := t.Pool.GetPage(int(rid.PageId))
	if err != nil {
		return err
	}

	heapPage := pages.AsHeapPage(page)
	page.WLatch()
	defer page.WUnlatch()
	defer t.Pool.Unpin(page.GetPageId(), true)
	if err := heapPage.UpdateTuple(int(rid.SlotIdx), tuple.GetData()); err != nil {
		// if error is because of tuple does not have enough space then update should do delete-insert
		return err
	}

	return nil
}

func (t *TableHeap) ReadTuple(rid Rid, dest *Row, txn concurrency.Transaction) error {
	p, err := t.Pool.GetPage(int(rid.PageId))
	if err != nil {
		return err
	}

	heapPage := pages.AsHeapPage(p)

	heapPage.RLatch()
	defer heapPage.RUnLatch()
	defer t.Pool.Unpin(heapPage.GetPageId(), false)

	data := heapPage.GetTuple(int(rid.SlotIdx))
	dest.Data = data
	dest.Rid = rid
	return nil
}

func (t *TableHeap) Vacuum() error {
	// TODO: should it have a transaction? it might be beneficial to have a special transaction for these kind of
	// background jobs so that they can work in parallel to other processes too.
	panic("implement me")
}

func (t *TableHeap) GetLastPage() (*pages.HeapPage, error) {
	rawPage, err := t.Pool.GetPage(t.LastPageID)
	if err != nil {
		return nil, err
	}

	return pages.AsHeapPage(rawPage), nil
}

func (t *TableHeap) GetFirstPage() (*pages.HeapPage, error) {
	rawPage, err := t.Pool.GetPage(t.FirstPageID)
	if err != nil {
		return nil, err
	}

	return pages.AsHeapPage(rawPage), nil
}

func NewTableHeap(pool *buffer.BufferPool, firstPageId int) *TableHeap {
	return &TableHeap{
		Pool:        pool,
		FirstPageID: firstPageId,
	}
}

func NewTableHeapWithTxn(pool *buffer.BufferPool, txn concurrency.Transaction) (*TableHeap, error) {
	p, err := pool.NewPage()
	if err != nil {
		return nil, err
	}
	defer pool.Unpin(p.GetPageId(), true)
	sp := pages.InitHeapPage(p)

	return &TableHeap{
		Pool:        pool,
		FirstPageID: sp.GetPageId(),
	}, nil
}
