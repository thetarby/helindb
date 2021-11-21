package pages

import (
	"helin/disk"
	"sync"
)

// IPage is a wrapper for actual physical pages in the file system. It can provide the actual content of the
// physical page as a byte array. It also keeps some useful information about the page for buffer pool.
type IPage interface {
	GetData() []byte

	// GetPageId returns the page_id of the physical page.
	GetPageId() int
	GetPinCount() int
	IsDirty() bool
	SetDirty()
	SetClean()
	WLatch()
	WUnlatch()
	RLatch()
	RUnLatch()
	IncrPinCount()
	DecrPinCount()
}

type RawPage struct {
	pageId   int
	isDirty  bool
	rwLatch  sync.RWMutex
	PinCount int
	Data     []byte
}

func NewRawPage(pageId int) *RawPage {
	return &RawPage{
		pageId:   pageId,
		isDirty:  false,
		rwLatch:  sync.RWMutex{},
		PinCount: 0,
		Data:     make([]byte, disk.PageSize, disk.PageSize),
	}
}

func (p *RawPage) IncrPinCount() {
	p.PinCount++
}

func (p *RawPage) DecrPinCount() {
	p.PinCount--
}

func (p *RawPage) GetData() []byte {
	return p.Data
}

func (p *RawPage) GetPageId() int {
	return p.pageId
}

func (p *RawPage) GetPinCount() int {
	return p.PinCount
}

func (p *RawPage) IsDirty() bool {
	return p.isDirty
}

func (p *RawPage) SetDirty() {
	p.isDirty = true
}

func (p *RawPage) SetClean() {
	p.isDirty = false
}

func (p *RawPage) WLatch() {
	p.rwLatch.Lock()
}

func (p *RawPage) WUnlatch() {
	p.rwLatch.Unlock()
}

func (p *RawPage) RLatch() {
	p.rwLatch.RLock()
}

func (p *RawPage) RUnLatch() {
	p.rwLatch.RUnlock()
}
