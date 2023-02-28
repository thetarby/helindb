package wal

import (
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
)

// LSP stands for logging slotted page. It wraps a slotted page and a log manager and logs all page modifying actions
// so that caller does not need to keep track of logging actions.
type LSP struct {
	pages.SlottedPage
	lm LogManager
}

func (p *LSP) InsertAt(txn transaction.Transaction, idx int, data []byte) error {
	if err := p.SlottedPage.InsertAt(idx, data); err != nil {
		return err
	}

	lsn := p.lm.AppendLog(NewInsertLogRecord(txn.GetID(), uint16(idx), data, p.GetPageId()))
	p.SetPageLSN(lsn)
	return nil
}

func (p *LSP) SetAt(txn transaction.Transaction, idx int, data []byte) error {
	old := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.SetAt(idx, data); err != nil {
		return err
	}

	lsn := p.lm.AppendLog(NewSetLogRecord(txn.GetID(), uint16(idx), data, old, p.GetPageId()))
	p.SetPageLSN(lsn)
	return nil
}

func (p *LSP) DeleteAt(txn transaction.Transaction, idx int) error {
	deleted := common.Clone(p.GetAt(idx))
	if err := p.SlottedPage.DeleteAt(idx); err != nil {
		return err
	}

	lsn := p.lm.AppendLog(NewDeleteLogRecord(txn.GetID(), uint16(idx), deleted, p.GetPageId()))
	p.SetPageLSN(lsn)
	return nil
}

func NewLSP(sp pages.SlottedPage, lm LogManager) LSP {
	return LSP{
		SlottedPage: sp,
		lm:          lm,
	}
}
