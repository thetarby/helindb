package btree

import (
	"helin/disk/pages"
	"helin/disk/wal"
)

// LoggedSlottedPage is almost same as pages.SlottedPage except that its GetPageId method returns a Pointer.
type LoggedSlottedPage struct {
	wal.LSP
}

func (sp *LoggedSlottedPage) GetPageId() Pointer {
	return Pointer(sp.SlottedPage.GetPageId())
}

func InitLoggedSlottedPage(p pages.IPage, lm wal.LogManager) LoggedSlottedPage {
	return LoggedSlottedPage{
		LSP: wal.NewLSP(pages.InitSlottedPage(p), lm),
	}
}

func CastLoggedSlottedPage(p pages.IPage, lm wal.LogManager) LoggedSlottedPage {
	return LoggedSlottedPage{
		LSP: wal.NewLSP(pages.CastSlottedPage(p), lm),
	}
}
