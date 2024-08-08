package heap

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"helin/transaction"
	"sync"
	"sync/atomic"
	"testing"
)

type testPage struct {
	data   []byte
	pageID uint64
	mut    *sync.Mutex
}

func (t *testPage) CopyAt(txn transaction.Transaction, offset uint16, data []byte) error {
	copy(t.data[offset:], data)
	return nil
}

func (t *testPage) GetData() []byte {
	return t.data
}

func (t *testPage) GetPageID() uint64 {
	return t.pageID
}

func (t *testPage) Release(dirty bool) {
	t.mut.Unlock()
}

var _ CopyPage = &testPage{}

type testPager struct {
	pages   map[uint64]*testPage
	counter atomic.Uint64
}

func newTestPager() *testPager {
	pages := make(map[uint64]*testPage)
	pages[0] = &testPage{
		data:   make([]byte, 4096*2),
		pageID: 0,
	}

	return &testPager{
		pages: pages,
	}
}

func (t *testPager) CreatePage(txn transaction.Transaction) (PageReleaser, error) {
	id := t.counter.Add(1)
	page := testPage{
		data:   make([]byte, 4096*2),
		pageID: id,
		mut:    &sync.Mutex{},
	}

	page.mut.Lock()

	t.pages[id] = &page
	return &page, nil
}

func (t *testPager) FreePage(txn transaction.Transaction, pageID uint64) error {
	delete(t.pages, pageID)
	return nil
}

func (t *testPager) GetPageToRead(pageID uint64) (PageReleaser, error) {
	page, ok := t.pages[pageID]
	if !ok {
		return nil, errors.New("page not found")
	}

	page.mut.Lock()
	return page, nil
}

func (t *testPager) GetPageToWrite(pageID uint64) (PageReleaser, error) {
	page, ok := t.pages[pageID]
	if !ok {
		return nil, errors.New("page not found")
	}

	page.mut.Lock()
	return page, nil
}

var _ Pager = &testPager{}

func TestHeap(t *testing.T) {
	h, err := InitHeap(transaction.TxnNoop(), 20, 4096*2, 4096*2, newTestPager())
	if err != nil {
		panic(err)
	}

	x := make([]byte, 6000)
	x = append(x, []byte("sa")...)

	if err := h.SetAt(transaction.TxnNoop(), 0, x); err != nil {
		panic(err)
	}

	b, err := h.GetAt(0)
	if err != nil {
		panic(err)
	}

	t.Logf("%v", string(b))
	assert.Equal(t, x, b)
}

func TestHeapCount(t *testing.T) {
	h, err := InitHeap(transaction.TxnNoop(), 20, 4096*2, 4096*2, newTestPager())
	if err != nil {
		panic(err)
	}

	x := make([]byte, 6000)
	x = append(x, []byte("sa")...)

	for i := 0; i < 10; i++ {
		if err := h.SetAt(transaction.TxnNoop(), 0, x); err != nil {
			panic(err)
		}
	}

	if err := h.FreeEmptyPages(transaction.TxnNoop()); err != nil {
		panic(err)
	}

	b, err := h.GetAt(0)
	assert.NoError(t, err)

	c, err := h.Count()
	assert.NoError(t, err)

	assert.Equal(t, 1, c)

	t.Logf("%v", string(b))
}
