package buffer

import (
	"helin/disk/pages"
	"helin/freelist"
)

/*
	this file includes an implementation of freelist.Pager using BufferPool. It is a bit tricky because buffer pool
	itself also uses free list. There might be a better way such as separating page-freeing logic from buffer pool and
	using free list everywhere needed explicitly but that seems like a lot of boilerplate with little to no real gain.
	(just like view models :P )
*/

var _ freelist.Pager = &pager{}

type pager struct {
	bp *BufferPool
}

func (p *pager) GetPage(pageId uint64) (*pages.RawPage, error) {
	return p.bp.getPage(pageId)
}

func (p *pager) Unpin(pageId uint64, isDirty bool) bool {
	return p.bp.unpin(pageId, isDirty)
}

func newPager(bp *BufferPool) *pager {
	return &pager{bp: bp}
}
