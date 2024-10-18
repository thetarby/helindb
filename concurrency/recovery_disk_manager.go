package concurrency

import (
	"errors"
	"helin/buffer"
	"helin/common"
	"helin/disk"
	"helin/disk/pages"
	"helin/transaction"
	"io"
)

type RecoveryDiskManager interface {
	// GetPage read and cast page to pages.SlottedPage, recovery does not support any other page type. So every page
	// that supports recovery in the db must be SlottedPage.
	GetPage(pageId uint64) (*pages.RawPage, error)
	Unpin(pageId uint64)
	FormatPage(pageId uint64, lsn pages.LSN) error
	FreePage(txn transaction.Transaction, pageID uint64) error
	GetFreeListLsn(txn transaction.Transaction) pages.LSN
}

type recoveryDiskManager struct {
	dm   *disk.Manager
	pool buffer.Pool
}

var _ RecoveryDiskManager = &recoveryDiskManager{}

func (d *recoveryDiskManager) Unpin(pageId uint64) {
	d.pool.Unpin(pageId, false)
}

func (d *recoveryDiskManager) FormatPage(pageId uint64, lsn pages.LSN) error {
	_, err := d.pool.GetPage(pageId)
	if err != io.EOF {
		return errors.New("tried to format a page but it is already allocated")
	}

	sp := pages.NewRawPage(pageId, disk.PageSize)
	sp.SetPageLSN(lsn)
	if err := d.dm.WritePage(sp.GetWholeData(), pageId); err != nil {
		return err
	}

	return nil
}

func (d *recoveryDiskManager) GetPage(pageId uint64) (*pages.RawPage, error) {
	common.Assert(pageId != 0, "db header page is accessed")

	p, err := d.pool.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (d *recoveryDiskManager) FreePage(txn transaction.Transaction, pageID uint64) error {
	return d.pool.FreePage(txn, pageID)
}

func (d *recoveryDiskManager) GetFreeListLsn(txn transaction.Transaction) pages.LSN {
	return d.pool.GetFreeList().GetHeaderPageLsn(txn)
}
