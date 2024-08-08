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

	page.WLatch()

	lsn := b.lm.AppendLog(txn, wal.NewPageFormatLogRecord(txn.GetID(), pages.TypeCopyAtPage, page.GetPageId()))

	cp := pages.InitCopyAtPage(page)
	cp.SetPageLSN(lsn)
	cp.SetDirty()

	return NewWriteHeapPageReleaser(&cp, b.pool, b.lm), nil
}

func (b *BufferPoolPager) FreePage(txn transaction.Transaction, pageID uint64) error {
	txn.FreePage(pageID)
	return nil
}

func (b *BufferPoolPager) GetPageToRead(pageID uint64) (heap.PageReleaser, error) {
	rp, err := b.pool.GetPage(pageID)
	if err != nil {
		return nil, err
	}

	rp.RLatch()
	cp := pages.CastCopyAtPage(rp)
	return NewReadHeapPageReleaser(&cp, b.pool, b.lm), nil
}

func (b *BufferPoolPager) GetPageToWrite(pageID uint64) (heap.PageReleaser, error) {
	page, err := b.pool.GetPage(pageID)
	if err != nil {
		return nil, err
	}

	page.WLatch()
	cp := pages.CastCopyAtPage(page)
	return NewWriteHeapPageReleaser(&cp, b.pool, b.lm), nil
}

type ReadHeapPageReleaser struct {
	*LoggedHeapPage
	pool buffer.Pool
}

func NewReadHeapPageReleaser(page *pages.CopyAtPage, pool buffer.Pool, lm wal.LogManager) *ReadHeapPageReleaser {
	return &ReadHeapPageReleaser{LoggedHeapPage: &LoggedHeapPage{p: page, lm: lm}, pool: pool}
}

var _ heap.PageReleaser = &ReadHeapPageReleaser{}

func (r *ReadHeapPageReleaser) Release(dirty bool) {
	r.pool.Unpin(r.p.PageId, false)
	r.p.RUnLatch()
}

type WriteHeapPageReleaser struct {
	*LoggedHeapPage
	pool buffer.Pool
}

func NewWriteHeapPageReleaser(page *pages.CopyAtPage, pool buffer.Pool, lm wal.LogManager) *WriteHeapPageReleaser {
	return &WriteHeapPageReleaser{LoggedHeapPage: &LoggedHeapPage{p: page, lm: lm}, pool: pool}
}

var _ heap.PageReleaser = &WriteHeapPageReleaser{}

func (r *WriteHeapPageReleaser) Release(dirty bool) {
	r.pool.Unpin(r.p.PageId, dirty)
	r.p.WUnlatch()
}
