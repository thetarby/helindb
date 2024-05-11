package freelist

import (
	"helin/common"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
)

type LSP struct {
	pages.SlottedPage
	lm wal.LogManager
}

func (p *LSP) SetAt(txn transaction.TxnID, idx int, data []byte) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lr := wal.NewSetLogRecord(txn, uint16(idx), data, old, p.GetPageId())
	lr.RedoOnly = true
	lsn := p.lm.AppendLog(lr)
	p.SetPageLSN(lsn)

	return nil
}

func (p *LSP) SetAtFree(txn transaction.TxnID, pageID uint64, idx int, data []byte, undoNext pages.LSN) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lr := wal.NewFreePageLogRecord(txn, uint16(idx), data, old, pageID)
	if lr.UndoNext != pages.ZeroLSN {
		lr.UndoNext = undoNext
		lr.IsClr = true
	}
	lsn := p.lm.AppendLog(lr)
	p.SetPageLSN(lsn)

	return nil
}

func (p *LSP) SetAtAlloc(txn transaction.TxnID, pageId uint64, idx int, data []byte, undoNext pages.LSN) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lr := wal.NewAllocPageLogRecord(txn, uint16(idx), data, old, pageId)
	if lr.UndoNext != pages.ZeroLSN {
		lr.UndoNext = undoNext
		lr.IsClr = true
	}
	lsn := p.lm.AppendLog(lr)
	p.SetPageLSN(lsn)

	return nil
}

func NewLSP(sp pages.SlottedPage, lm wal.LogManager) LSP {
	return LSP{
		SlottedPage: sp,
		lm:          lm,
	}
}
