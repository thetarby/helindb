package pages

import (
	"encoding/binary"
	"sync"
)

// IPage is a wrapper for actual physical pages in the file system. It can provide the actual content of the
// physical page as a byte array. It also keeps some useful information about the page for buffer pool.
type IPage interface {
	// GetData returns usable space.
	GetData() []byte

	// GetWholeData returns whole content of the page including headers.
	GetWholeData() []byte

	// GetPageId returns the page_id of the physical page.
	GetPageId() uint64
	GetPinCount() int
	IsDirty() bool
	SetDirty()
	SetClean()
	Clear()
	WLatch()
	WUnlatch()
	RLatch()
	RUnLatch()
	TryRLatch() bool
	TryWLatch() bool
	IncrPinCount()
	DecrPinCount()
	SetType(l uint64)
	GetType() uint64
	SetPageLSN(l LSN)
	GetPageLSN() LSN
}

var _ IPage = &RawPage{}

type RawPage struct {
	PageId   uint64
	isDirty  bool
	rwLatch  *sync.RWMutex
	PinCount int
	Data     []byte
	trace    bool
}

func NewRawPage(pageId uint64, pageSize int) *RawPage {
	return &RawPage{
		PageId:   pageId,
		isDirty:  false,
		rwLatch:  &sync.RWMutex{},
		PinCount: 0,
		Data:     make([]byte, pageSize, pageSize),
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
	// NOTE: it would be really good for debugging if this method can recognize whether buffer pool has replaced
	// underlying page with another physical page. pages may contain their id for example and this method checks
	// id in raw bytes with the id struct holds?
	return p.Data[16:]
}

func (p *RawPage) GetWholeData() []byte {
	// NOTE: it would be really good for debugging if this method can recognize whether buffer pool has replaced
	// underlying page with another physical page. pages may contain their id for example and this method checks
	// id in raw bytes with the id struct holds?
	return p.Data
}

func (p *RawPage) GetPageId() uint64 {
	return p.PageId
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

func (p *RawPage) Clear() {
	for i := 0; i < len(p.Data); i++ {
		p.Data[i] = 0
	}
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

func (p *RawPage) TryRLatch() bool {
	return p.rwLatch.TryRLock()
}

func (p *RawPage) TryWLatch() bool {
	return p.rwLatch.TryLock()
}

func (p *RawPage) SetType(t uint64) {
	binary.BigEndian.PutUint64(p.Data, t)
}

func (p *RawPage) GetType() uint64 {
	return binary.BigEndian.Uint64(p.Data)
}

func (p *RawPage) SetPageLSN(l LSN) {
	binary.BigEndian.PutUint64(p.Data[8:], uint64(l))
}

func (p *RawPage) GetPageLSN() LSN {
	return LSN(binary.BigEndian.Uint64(p.Data[8:]))
}
