package freelist

import (
	"helin/disk/pages"
	"helin/disk/wal"
)

type flPager struct {
	bp Pager
	lm wal.LogManager
}

func (p *flPager) GetPageToRead(pageId uint64) (*readPage, error) {
	rp, err := p.bp.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	lsp := wal.NewLSP(pages.CastSlottedPage(rp), p.lm)
	return &readPage{lsp, p.bp}, nil
}

func (p *flPager) GetPageToWrite(pageId uint64) (*writePage, error) {
	rp, err := p.bp.GetPage(pageId)
	if err != nil {
		return nil, err
	}

	// IMPORTANT NOTE: free list does not take locks on pages since transactions can keep holding a lock on a page
	// after freeing it. (locks are released when txn commits) Instead, synchronization is achieved by freelist's own
	// mutex. This means a transaction can keep holding lock on a page after freeing but should not make any modifications
	// on it because it may interfere with freelist's modification. In that case lock is only hold to avoid another
	// transaction to alloc the same freed page and modify it before freeing transaction commits.
	lsp := wal.NewLSP(pages.CastSlottedPage(rp), p.lm)
	return &writePage{lsp, p.bp}, nil
}

type readPage struct {
	wal.LSP
	pager Pager
}

func (r *readPage) Release() {
	r.pager.Unpin(r.GetPageId(), false)
}

type writePage struct {
	wal.LSP
	pager Pager
}

func (r *writePage) Release() {
	r.pager.Unpin(r.GetPageId(), true)
}
