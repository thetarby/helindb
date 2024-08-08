package btree

import (
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
)

var _ BPage = &LoggedSlottedPage{}

// LoggedSlottedPage is a BPage implementation utilizing pages.SlottedPage
type LoggedSlottedPage struct {
	wal.LSP
}

func (sp *LoggedSlottedPage) GetPageId() Pointer {
	return Pointer(sp.SlottedPage.GetPageId())
}

func InitLoggedSlottedPage(txn transaction.Transaction, p pages.IPage, lm wal.LogManager) *LoggedSlottedPage {
	// TODO: no write latch?
	lsn := lm.AppendLog(txn, wal.NewPageFormatLogRecord(txn.GetID(), pages.TypeSlottedPage, p.GetPageId()))

	sp := pages.InitSlottedPage(p)
	sp.SetPageLSN(lsn)
	sp.SetDirty()

	return &LoggedSlottedPage{
		LSP: wal.NewLSP(sp, lm),
	}
}

func CastLoggedSlottedPage(p pages.IPage, lm wal.LogManager) LoggedSlottedPage {
	return LoggedSlottedPage{
		LSP: wal.NewLSP(pages.CastSlottedPage(p), lm),
	}
}
