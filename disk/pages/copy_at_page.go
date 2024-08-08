package pages

import "helin/common"

type CopyAtPage struct {
	*RawPage
}

func (h *CopyAtPage) CopyAt(offset uint16, data []byte) {
	d := h.GetData()
	copy(d[offset:], data)
}

func (h *CopyAtPage) ReadAt(offset uint16, len int) []byte {
	d := h.GetData()

	return d[offset : int(offset)+len]
}

func InitCopyAtPage(p *RawPage) CopyAtPage {
	common.ZeroBytes(p.GetData())
	p.SetType(TypeCopyAtPage)
	return CopyAtPage{RawPage: p}
}

func CastCopyAtPage(p *RawPage) CopyAtPage {
	common.Assert(p.GetType() == TypeCopyAtPage, "page is not a slotted page, it is %v", p.GetType())
	return CopyAtPage{RawPage: p}
}
