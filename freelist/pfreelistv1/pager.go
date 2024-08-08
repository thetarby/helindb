package pfreelistv1

import (
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/freelist/freelistv1"
	"helin/transaction"
)

type Pool interface {
	GetPage(pageId uint64) (*pages.RawPage, error)
	Unpin(pageId uint64, isDirty bool) bool
}

type BufferPoolPager struct {
	pool Pool
	lm   wal.LogManager
}

func NewBufferPoolPager(pool Pool, lm wal.LogManager) *BufferPoolPager {
	return &BufferPoolPager{pool: pool, lm: lm}
}

func (b *BufferPoolPager) GetPageToRead(pageId uint64) (freelistv1.FreeListPage, error) {
	rp, err := b.pool.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	rp.WLatch()

	return newLoggedFreelistPage(rp, b.pool, b.lm), nil
}

func (b *BufferPoolPager) GetPageToWrite(txn transaction.Transaction, pageId uint64, format bool) (freelistv1.FreeListPage, error) {
	rp, err := b.pool.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	rp.WLatch()

	// TODO IMPORTANT: might net be a slotted page if it is tail page
	if format {
		return initLoggedFreelistPage(txn, rp, b.pool, b.lm), nil
	} else {
		return newLoggedFreelistPage(rp, b.pool, b.lm), nil
	}
}

var _ freelistv1.Pager = &BufferPoolPager{}
