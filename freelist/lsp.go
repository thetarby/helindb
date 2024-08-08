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

func (p *LSP) SetAt(txn transaction.Transaction, idx int, data []byte) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lr := wal.NewFreePageSetLogRecord(txn.GetID(), uint16(idx), data, old, p.GetPageId())
	lsn := p.lm.AppendLog(txn, lr)
	p.SetPageLSN(lsn)

	return nil
}

func (p *LSP) SetAtAndAppendFreeLog(txn transaction.Transaction, idx int, data []byte, pageID uint64, undoNext pages.LSN) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lr := wal.NewFreePageLogRecord(txn.GetID(), uint16(idx), data, old, pageID)
	if lr.UndoNext != pages.ZeroLSN {
		lr.UndoNext = undoNext
		lr.IsClr = true
	}
	lsn := p.lm.AppendLog(txn, lr)
	p.SetPageLSN(lsn)

	return nil
}

func (p *LSP) SetAtAndAppendAllocLog(txn transaction.Transaction, idx int, data []byte, pageId uint64, undoNext pages.LSN) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lr := wal.NewAllocPageLogRecord(txn.GetID(), uint16(idx), data, old, pageId)
	if lr.UndoNext != pages.ZeroLSN {
		lr.UndoNext = undoNext
		lr.IsClr = true
	}
	lsn := p.lm.AppendLog(txn, lr)
	p.SetPageLSN(lsn)

	return nil
}

func NewLSP(sp pages.SlottedPage, lm wal.LogManager) LSP {
	return LSP{
		SlottedPage: sp,
		lm:          lm,
	}
}
