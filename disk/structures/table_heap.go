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
	InsertTuple(tuple Tuple, txn concurrency.Transaction) (Rid, error)

	// UpdateTuple if the new tuple is too large to fit in the old page, return error (will delete and insert)
	UpdateTuple(tuple Tuple, rid Rid, txn concurrency.Transaction) error

	// ReadTuple if tuple does not exist at rid returns an error
	ReadTuple(rid Rid, dest *Tuple, txn concurrency.Transaction) error

	// HardDeleteTuple if tuple does not exist at rid returns an error
	HardDeleteTuple(rid Rid, txn concurrency.Transaction) error

	// Vacuum compresses the structure so that there are no gaps between pages and in pages.
	Vacuum() error
}

type TableHeap struct {
	pool        *buffer.BufferPool
	firstPageID int
	lastPageID  int
}

func (t *TableHeap) HardDeleteTuple(rid Rid, txn concurrency.Transaction) error {
	page, err := t.pool.GetPage(int(rid.PageId))
	if err != nil {
		return err
	}

	slottedPage := pages.SlottedPageInstanceFromRawPage(page)
	if err := slottedPage.HardDelete(int(rid.SlotIdx)); err != nil {
		return err
	}

	return nil
}

func (t *TableHeap) InsertTuple(tuple Tuple, txn concurrency.Transaction) (Rid, error) {
	// TODO: unpin pages
	currPage, err := t.GetFirstPage()
	if err != nil {
		return Rid{}, err
	}

	for {
		// if there is enough space in the current page insert tuple and return rid
		if currPage.GetFreeSpace() >= (tuple.Length())+pages.SLOT_ARRAY_ENTRY_SIZE {
			idx, err := currPage.InsertTuple(tuple.GetData())
			if err != nil {
				return Rid{}, err
			}
			t.pool.Unpin(currPage.GetPageId(), true)
			return NewRid(currPage.GetPageId(), idx), nil
		}

		// else get next page and try again
		if currPage.GetHeader().NextPageID == 0 {
			page, err := t.pool.NewPage()
			if err != nil {
				return Rid{}, err
			}

			currPage.WLatch()
			h := currPage.GetHeader()
			h.NextPageID = int64(page.GetPageId())
			currPage.SetHeader(h)
			currPage.WUnlatch()

			t.pool.Unpin(currPage.GetPageId(), true)
			currPage = pages.FormatAsSlottedPage(page)
			continue
		}

		// if next page id is set move on to that page
		t.pool.Unpin(currPage.GetPageId(), false)
		raw, err := t.pool.GetPage(int(currPage.GetHeader().NextPageID))
		if err != nil {
			return Rid{}, err
		}
		currPage = pages.SlottedPageInstanceFromRawPage(raw)
	}
}

func (t *TableHeap) UpdateTuple(tuple Tuple, rid Rid, txn concurrency.Transaction) error {
	page, err := t.pool.GetPage(int(rid.PageId))
	if err != nil {
		return err
	}

	slottedPage := pages.SlottedPageInstanceFromRawPage(page)
	if err := slottedPage.UpdateTuple(int(rid.SlotIdx), tuple.GetData()); err != nil {
		// if error is because of tuple does not have enough space then update should do delete-insert
		return err
	}

	return nil
}

func (t *TableHeap) ReadTuple(rid Rid, dest *Tuple, txn concurrency.Transaction) error {
	p, err := t.pool.GetPage(int(rid.PageId))
	if err != nil {
		return err
	}

	slottedPage := pages.SlottedPageInstanceFromRawPage(p)
	data := slottedPage.GetTuple(int(rid.SlotIdx))
	dest.data = data
	t.pool.Unpin(p.GetPageId(), false)
	return nil
}

func (t *TableHeap) Vacuum() error {
	// TODO: should it have a transaction? it might be beneficial to have a special transaction for these kind of
	// background jobs so that they can work in parallel to other processes too.
	panic("implement me")
}

func (t *TableHeap) GetLastPage() (*pages.SlottedPage, error) {
	rawPage, err := t.pool.GetPage(t.lastPageID)
	if err != nil {
		return nil, err
	}
	slottedPage := pages.SlottedPageInstanceFromRawPage(rawPage)

	return slottedPage, nil
}

func (t *TableHeap) GetFirstPage() (*pages.SlottedPage, error) {
	rawPage, err := t.pool.GetPage(t.firstPageID)
	if err != nil {
		return nil, err
	}
	slottedPage := pages.SlottedPageInstanceFromRawPage(rawPage)

	return slottedPage, nil
}
