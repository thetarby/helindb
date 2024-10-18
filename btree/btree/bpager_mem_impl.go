package btree

import (
	"errors"
	"helin/disk/pages"
	"helin/transaction"
	"sync"
)

var _ BPager = &MemBPager{}

type MemBPager struct {
	mut           *sync.Mutex
	pageIDCounter uint64
	mapping       map[Pointer]*memBPage
	mapping2      map[Pointer]*memOverFlow
	pageSize      int
}

func NewMemBPager(pageSize int) *MemBPager {
	return &MemBPager{
		mut:           &sync.Mutex{},
		pageIDCounter: 0,
		mapping:       make(map[Pointer]*memBPage),
		mapping2:      make(map[Pointer]*memOverFlow),
		pageSize:      pageSize,
	}
}

func (b *MemBPager) NewBPage(txn transaction.Transaction) (BPageReleaser, error) {
	b.mut.Lock()
	defer b.mut.Unlock()

	b.pageIDCounter++

	page := newMemBPage(b.pageIDCounter, b.pageSize)
	page.WLatch()

	b.mapping[Pointer(b.pageIDCounter)] = page

	return &writeBpageReleaser{page, b}, nil
}

func (b *MemBPager) GetBPageToRead(txn transaction.Transaction, pointer Pointer) (BPageReleaser, error) {
	b.mut.Lock()

	p, ok := b.mapping[pointer]
	if !ok {
		b.mut.Unlock()
		return nil, errors.New("page not found")
	}

	b.mut.Unlock()
	p.RLatch()
	return &readBpageReleaser{p, b}, nil
}

func (b *MemBPager) GetBPageToWrite(txn transaction.Transaction, pointer Pointer) (BPageReleaser, error) {
	b.mut.Lock()

	p, ok := b.mapping[pointer]
	if !ok {
		b.mut.Unlock()
		return nil, errors.New("page not found")
	}

	b.mut.Unlock()
	p.WLatch()
	return &writeBpageReleaser{p, b}, nil
}

func (b *MemBPager) GetBPage(txn transaction.Transaction, pointer Pointer) (BPage, error) {
	b.mut.Lock()
	defer b.mut.Unlock()

	p, ok := b.mapping[pointer]
	if !ok {
		return nil, errors.New("page not found")
	}

	return p, nil
}

func (b *MemBPager) Unpin(p Pointer) {
	// can be noop for in memory
}

func (b *MemBPager) FreeBPage(txn transaction.Transaction, p Pointer) {
	b.mut.Lock()
	defer b.mut.Unlock()

	delete(b.mapping, p)
}

func (b *MemBPager) CreateOverflow(txn transaction.Transaction) (OverflowReleaser, error) {
	b.mut.Lock()
	defer b.mut.Unlock()

	b.pageIDCounter++

	of := newMemOverFlow(b.pageIDCounter)
	b.mapping2[Pointer(b.pageIDCounter)] = of

	return of, nil
}

func (b *MemBPager) FreeOverflow(txn transaction.Transaction, p Pointer) error {
	b.mut.Lock()
	defer b.mut.Unlock()

	delete(b.mapping2, p)
	return nil
}

func (b *MemBPager) GetOverflowReleaser(p Pointer) (OverflowReleaser, error) {
	b.mut.Lock()
	defer b.mut.Unlock()

	return b.mapping2[p], nil
}

var _ BPage = &memBPage{}

type memBPage struct {
	pages.SlottedPage
}

func newMemBPage(pageID uint64, pageSize int) *memBPage {
	return &memBPage{
		SlottedPage: pages.InitSlottedPage(pages.NewRawPage(pageID, pageSize)),
	}
}

func (m *memBPage) DeleteAt(txn transaction.Transaction, idx int) error {
	return m.SlottedPage.DeleteAt(idx)
}

func (m *memBPage) InsertAt(txn transaction.Transaction, idx int, data []byte) error {
	return m.SlottedPage.InsertAt(idx, data)
}

func (m *memBPage) SetAt(txn transaction.Transaction, idx int, data []byte) error {
	return m.SlottedPage.SetAt(idx, data)
}

func (m *memBPage) GetPageId() Pointer {
	return Pointer(m.SlottedPage.GetPageId())
}

type readBpageReleaser struct {
	*memBPage
	bpager BPager
}

func (n *readBpageReleaser) Release() {
	n.bpager.Unpin(n.GetPageId())
	n.RUnLatch()
}

type writeBpageReleaser struct {
	*memBPage
	bpager BPager
}

func (n *writeBpageReleaser) Release() {
	n.bpager.Unpin(n.GetPageId())
	n.WUnlatch()
}

var _ OverflowReleaser = &memOverFlow{}

type memOverFlow struct {
	pageID uint64

	data       map[int][]byte
	mut        *sync.Mutex
	idxCounter int
}

func newMemOverFlow(pageID uint64) *memOverFlow {
	return &memOverFlow{
		pageID: pageID,
		data:   make(map[int][]byte),
		mut:    &sync.Mutex{},
	}
}

func (m *memOverFlow) GetPageId() uint64 {
	m.mut.Lock()
	defer m.mut.Unlock()

	return m.pageID
}

func (m *memOverFlow) GetAt(txn transaction.Transaction, idx int) ([]byte, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	d, ok := m.data[idx]
	if !ok {
		return nil, errors.New("item cannot be found")
	}

	return d, nil
}

func (m *memOverFlow) Insert(txn transaction.Transaction, data []byte) (int, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.idxCounter++
	m.data[m.idxCounter] = data

	return m.idxCounter, nil
}

func (m *memOverFlow) SetAt(txn transaction.Transaction, idx int, data []byte) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	m.data[idx] = data
	return nil
}

func (m *memOverFlow) DeleteAt(txn transaction.Transaction, idx int) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	_, ok := m.data[idx]
	if !ok {
		panic("attempts to delete not existing item")
	}

	delete(m.data, idx)
	return nil
}

func (m *memOverFlow) Count(txn transaction.Transaction) (int, error) {
	m.mut.Lock()
	defer m.mut.Unlock()

	return len(m.data), nil
}

func (m *memOverFlow) Free(txn transaction.Transaction) error {
	return nil
}
