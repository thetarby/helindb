package freelist

import (
	"helin/disk/pages"
	"helin/disk/wal"
)

type Pager interface {
	GetPage(pageId uint64) (*pages.RawPage, error)
	Unpin(pageId uint64, isDirty bool) bool
}

type flPager struct {
	bp Pager
	lm wal.LogManager
}

func (p *flPager) GetPageToRead(pageId uint64) (*readPage, error) {
	rp, err := p.bp.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	lsp := NewLSP(pages.CastSlottedPage(rp), p.lm)
	return &readPage{lsp, p.bp}, nil
}

func (p *flPager) GetPageToWrite(pageId uint64) (*writePage, error) {
	rp, err := p.bp.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	rp.WLatch()
	lsp := NewLSP(pages.CastSlottedPage(rp), p.lm)
	return &writePage{lsp, p.bp}, nil
}

type readPage struct {
	LSP
	pager Pager
}

func (r *readPage) Release() {
	r.pager.Unpin(r.GetPageId(), false)
	r.RUnLatch()
}

type writePage struct {
	LSP
	pager Pager
}

func (r *writePage) Release() {
	r.pager.Unpin(r.GetPageId(), true)
	r.WUnlatch()
}
