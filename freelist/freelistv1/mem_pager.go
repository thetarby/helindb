package freelistv1

import "helin/transaction"

type memPager struct {
	pages map[uint64]*memPage
}

func newMemPager() *memPager {
	return &memPager{
		pages: make(map[uint64]*memPage),
	}
}

func (m *memPager) GetPageToRead(txn transaction.Transaction, pageId uint64) (FreeListPage, error) {
	p, ok := m.pages[pageId]
	if !ok {
		p = newMemPage(pageId)
		m.pages[pageId] = p
	}

	return p, nil
}

func (m *memPager) GetPageToWrite(txn transaction.Transaction, pageId uint64, format bool) (FreeListPage, error) {
	p, ok := m.pages[pageId]
	if !ok {
		p = newMemPage(pageId)
		m.pages[pageId] = p
	}

	return p, nil
}

var _ Pager = &memPager{}

type memPage struct {
	data   []byte
	pageID uint64
	lsn    uint64
}

func newMemPage(pageID uint64) *memPage {
	return &memPage{
		data:   nil,
		pageID: pageID,
		lsn:    0,
	}
}

func (m *memPage) Get() []byte {
	return m.data
}

func (m *memPage) Set(txn transaction.Transaction, bytes []byte, op *OpLog) error {
	m.data = make([]byte, len(bytes))
	copy(m.data, bytes)
	return nil
}

func (m *memPage) GetPageId() uint64 {
	return m.pageID
}

func (m *memPage) SetLSN(lsn uint64) {
	m.lsn = lsn
}

func (m *memPage) GetLSN() uint64 {
	return m.lsn
}

func (m *memPage) Release() {
	return
}

var _ FreeListPage = &memPage{}
