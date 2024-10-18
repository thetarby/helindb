package pheap

import (
	"helin/buffer"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/heap/heap"
	"helin/transaction"
)

type BufferPoolPager struct {
	pool buffer.Pool
	lm   wal.LogManager
}

func NewBufferPoolPager(pool buffer.Pool, lm wal.LogManager) *BufferPoolPager {
	return &BufferPoolPager{pool: pool, lm: lm}
}

var _ heap.Pager = &BufferPoolPager{}

func (b *BufferPoolPager) CreatePage(txn transaction.Transaction) (heap.PageReleaser, error) {
	page, err := b.pool.NewPage(txn)
	if err != nil {
		return nil, err
	}

	// page.WLatch()
	if err := txn.AcquireLatch(page.GetPageId(), transaction.Exclusive); err != nil {
		return nil, err
	}

	lsn := b.lm.AppendLog(txn, wal.NewPageFormatLogRecord(txn.GetID(), pages.TypeCopyAtPage, page.GetPageId()))

	cp := pages.InitCopyAtPage(page)
	cp.SetPageLSN(lsn)
	cp.SetDirty()

	return NewWriteHeapPageReleaser(&cp, b.pool, b.lm, txn), nil
}

func (b *BufferPoolPager) FreePage(txn transaction.Transaction, pageID uint64) error {
	txn.FreePage(pageID)
	return nil
}

func (b *BufferPoolPager) GetPageToRead(txn transaction.Transaction, pageID uint64) (heap.PageReleaser, error) {
	rp, err := b.pool.GetPage(pageID)
	if err != nil {
		return nil, err
	}

	//rp.RLatch()
	if err := txn.AcquireLatch(pageID, transaction.Shared); err != nil {
		return nil, err
	}

	cp := pages.CastCopyAtPage(rp)
	return NewReadHeapPageReleaser(&cp, b.pool, b.lm, txn), nil
}

func (b *BufferPoolPager) GetPageToWrite(txn transaction.Transaction, pageID uint64) (heap.PageReleaser, error) {
	page, err := b.pool.GetPage(pageID)
	if err != nil {
		return nil, err
	}

	//page.WLatch()
	if err := txn.AcquireLatch(pageID, transaction.Exclusive); err != nil {
		return nil, err
	}

	cp := pages.CastCopyAtPage(page)
	return NewWriteHeapPageReleaser(&cp, b.pool, b.lm, txn), nil
}

type ReadHeapPageReleaser struct {
	*LoggedHeapPage
	pool buffer.Pool
	txn  transaction.Transaction
}

func NewReadHeapPageReleaser(page *pages.CopyAtPage, pool buffer.Pool, lm wal.LogManager, txn transaction.Transaction) *ReadHeapPageReleaser {
	return &ReadHeapPageReleaser{LoggedHeapPage: &LoggedHeapPage{p: page, lm: lm}, pool: pool, txn: txn}
}

var _ heap.PageReleaser = &ReadHeapPageReleaser{}

func (r *ReadHeapPageReleaser) Release(dirty bool) {
	r.pool.Unpin(r.p.PageId, false)
	// r.p.RUnLatch()
	r.txn.ReleaseLatch(r.GetPageID())
}

type WriteHeapPageReleaser struct {
	*LoggedHeapPage
	pool buffer.Pool
	txn  transaction.Transaction
}

func NewWriteHeapPageReleaser(page *pages.CopyAtPage, pool buffer.Pool, lm wal.LogManager, txn transaction.Transaction) *WriteHeapPageReleaser {
	return &WriteHeapPageReleaser{LoggedHeapPage: &LoggedHeapPage{p: page, lm: lm}, pool: pool, txn: txn}
}

var _ heap.PageReleaser = &WriteHeapPageReleaser{}

func (r *WriteHeapPageReleaser) Release(dirty bool) {
	r.pool.Unpin(r.p.PageId, dirty)
	// r.p.WUnlatch()
	r.txn.ReleaseLatch(r.GetPageID())
}
