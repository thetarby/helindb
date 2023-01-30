package wal

import (
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
)

type LSP struct {
	pages.SlottedPage
	lm *LogManager
}

func (p *LSP) InsertAt(txn transaction.Transaction, idx int, data []byte) error {
	p.lm.AppendLog(NewInsertLogRecord(txn.GetID(), uint16(idx), data))
	return p.SlottedPage.InsertAt(idx, data)
}

func (p *LSP) SetAt(txn transaction.Transaction, idx int, data []byte) error {
	old := common.Clone(p.GetAt(idx))
	p.lm.AppendLog(NewSetLogRecord(txn.GetID(), uint16(idx), data, old))
	return p.SlottedPage.SetAt(idx, data)
}

func (p *LSP) DeleteAt(txn transaction.Transaction, idx int) error {
	deleted := common.Clone(p.GetAt(idx))
	p.lm.AppendLog(NewDeleteLogRecord(txn.GetID(), uint16(idx), deleted))
	return p.SlottedPage.DeleteAt(idx)
}

func NewLSP(sp pages.SlottedPage, lm *LogManager) LSP {
	return LSP{
		SlottedPage: sp,
		lm:          lm,
	}
}
