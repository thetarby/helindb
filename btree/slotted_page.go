package btree

import (
	"helin/disk/pages"
)

// SlottedPage is almost same as pages.SlottedPage except that its GetPageId method returns a Pointer.
type SlottedPage struct {
	pages.SlottedPage
}

func (sp *SlottedPage) GetPageId() Pointer {
	return Pointer(sp.SlottedPage.GetPageId())
}

func InitSlottedPage(p pages.IPage) SlottedPage {
	return SlottedPage{
		pages.InitSlottedPage(p),
	}
}

func CastSlottedPage(p pages.IPage) SlottedPage {
	return SlottedPage{
		pages.CastSlottedPage(p),
	}
}
