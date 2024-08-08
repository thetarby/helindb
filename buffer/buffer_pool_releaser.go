package buffer

import (
	"helin/disk/pages"
	"helin/transaction"
)

const (
	Read = iota
	Write
)

func (b *PoolV2) GetPageReleaser(pageId uint64, mode int) (PageReleaser, error) {
	p, err := b.GetPage(pageId)
	if err != nil {
		return nil, err
	}
	if mode == Read {
		p.RLatch()
		return &readPageReleaser{p, b}, nil
	} else {
		p.WLatch()
		return &writePageReleaser{p, b}, nil
	}
}

func (b *PoolV2) NewPageWithReleaser(txn transaction.Transaction) (page PageReleaser, err error) {
	p, err := b.NewPage(txn)
	if err != nil {
		return nil, err
	}
	p.WLatch()
	return &writePageReleaser{p, b}, nil
}

type PageReleaser interface {
	pages.IPage
	Release(dirty bool)
}

type readPageReleaser struct {
	pages.IPage
	pool *PoolV2
}

func (n *readPageReleaser) Release(bool) {
	n.pool.Unpin(n.GetPageId(), false)
	n.RUnLatch()
}

type writePageReleaser struct {
	pages.IPage
	pool *PoolV2
}

func (n *writePageReleaser) Release(isDirty bool) {
	n.pool.Unpin(n.GetPageId(), false)
	n.WUnlatch()
}
