package pages

import (
	"encoding/binary"
	"helin/disk"
	"sync"
)

// IPage is a wrapper for actual physical pages in the file system. It can provide the actual content of the
// physical page as a byte array. It also keeps some useful information about the page for buffer pool.
type IPage interface {
	GetData() []byte

	// GetPageId returns the page_id of the physical page.
	GetPageId() uint64
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
	SetRecLSN(l LSN)
	GetRecLSN() LSN
}

type RawPage struct {
	pageId   uint64
	isDirty  bool
	rwLatch  *sync.RWMutex
	PinCount int
	Data     []byte
	trace    bool
}

func NewRawPage(pageId uint64) *RawPage {
	return &RawPage{
		pageId:   pageId,
		isDirty:  false,
		rwLatch:  &sync.RWMutex{},
		PinCount: 0,
		Data:     make([]byte, disk.PageSize, disk.PageSize),
	}
}

func (p *RawPage) IncrPinCount() {
	p.PinCount++
}

func (p *RawPage) DecrPinCount() {
	p.PinCount--
	if p.PinCount < 0 {
		panic("negative pin count")
	}
}

func (p *RawPage) GetData() []byte {
	// TODO: it would be really good for debugging if this method can recognize whether buffer pool has replaced
	// underlying page with another physical page. pages may contain their id for example and this method checks
	// id in raw bytes with the id struct holds?
	return p.Data[16:]
}

func (p *RawPage) GetPageId() uint64 {
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

func (p *RawPage) SetRecLSN(l LSN) {
	binary.BigEndian.PutUint64(p.Data, uint64(l))
}

func (p *RawPage) SetPageLSN(l LSN) {
	binary.BigEndian.PutUint64(p.Data[8:], uint64(l))
}

func (p *RawPage) GetRecLSN() LSN {
	return LSN(binary.BigEndian.Uint64(p.Data))
}

func (p *RawPage) GetPageLSN() LSN {
	return LSN(binary.BigEndian.Uint64(p.Data[8:]))
}
