package concurrency

import (
	"errors"
	"helin/buffer"
	"helin/disk"
	"helin/disk/pages"
	"helin/transaction"
	"io"
)

type diskManager struct {
	dm   *disk.Manager
	pool buffer.Pool
}

var _ DiskManager = &diskManager{}

func (d *diskManager) Unpin(pageId uint64) {
	d.pool.Unpin(pageId, false)
}

func (d *diskManager) NewPage(pageId uint64) (*pages.SlottedPage, error) {
	_, err := d.pool.GetPage(pageId)
	if err != io.EOF {
		return nil, errors.New("tried to allocate a page but it is already allocated")
	}

	// TODO: it may not be a slotted page.
	sp := pages.InitSlottedPage(pages.NewRawPage(pageId, disk.PageSize))
	if err := d.dm.WritePage(sp.GetWholeData(), pageId); err != nil {
		return nil, err
	}

	// to place it in pool try fetching again
	p, err := d.pool.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	x := pages.CastSlottedPage(p)
	return &x, nil
}

func (d *diskManager) GetPage(pageId uint64) (*pages.SlottedPage, error) {
	p, err := d.pool.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	sp := pages.CastSlottedPage(p)
	return &sp, nil
}

func (d *diskManager) FreePage(txn transaction.Transaction, pageID uint64) {
	d.pool.GetFreeList().Add(txn, pageID)
}

func (d *diskManager) FreePageInRecovery(txn transaction.Transaction, pageID uint64, undoNext pages.LSN) {
	d.pool.GetFreeList().AddInRecovery(txn, pageID, undoNext)
}

func (d *diskManager) GetFreeListLsn() pages.LSN {
	return d.pool.GetFreeList().GetHeaderPageLsn()
}
