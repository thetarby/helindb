package freelistv1

import (
	"encoding/binary"
	"helin/common"
	"helin/transaction"
	"sync"
)

type LSN uint64

const (
	HeaderPageID = uint64(1)
)

/*
	Freelist operations are atomic. Pop and Add operations appends page free and alloc log records. If header page lsn
	is greater than any page free or alloc log record's lsn, it can safely be considered fully committed. Otherwise,
	it is not committed.
*/

type Header struct {
	freeListHead uint64
	freeListTail uint64
	catalogPID   uint64
}

func (h Header) Bytes() []byte {
	panic("implement me")
}

type List struct {
	lock   sync.Mutex
	header *Header
	pager  Pager
}

func NewFreeList(p Pager, init bool) *List {
	list := &List{
		lock:  sync.Mutex{},
		pager: p,
	}

	if init {
		list.initHeader()
	}

	return list
}

// Pop pops a page from free list. Unlike Add, this operation can be undone by simply adding page back to free list.
func (f *List) Pop(txn transaction.Transaction) (pageId uint64, err error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// if list is empty return 0
	h := f.getHeader()
	if h.freeListHead == 0 {
		return 0, nil
	}

	// if there is only one entry in free list return that and set head and tail to 0
	if h.freeListHead == h.freeListTail {
		pageId = h.freeListHead
		h.freeListHead, h.freeListTail = 0, 0

		// get and update header page
		hp, err := f.pager.GetPageToWrite(txn, HeaderPageID, false)
		if err != nil {
			return 0, err
		}
		defer hp.Release()

		f.setHeader(txn, hp, h, &OpLog{PageID: pageId, Op: OpPop})

		return pageId, nil
	}

	// else pop head, read new head and update header
	pageId = h.freeListHead

	popped, err := f.pager.GetPageToRead(h.freeListHead)
	if err != nil {
		return 0, err
	}
	defer popped.Release()

	hp, err := f.pager.GetPageToWrite(txn, HeaderPageID, false)
	if err != nil {
		return 0, err
	}
	defer hp.Release()

	h.freeListHead = binary.BigEndian.Uint64(popped.Get())

	f.setHeader(txn, hp, h, &OpLog{PageID: pageId, Op: OpPop})

	return
}

// Add adds page to free list. Note that this operation converts pages to SlottedPage and conversion operations are
// not logged, meaning this operation cannot be undone. Transactions should only free pages after they are committed
// so that page free operations are never rolled back.
func (f *List) Add(txn transaction.Transaction, pageId uint64) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	h := f.getHeader()

	// if free list is empty
	if h.freeListHead == 0 {
		h.freeListHead = pageId
		h.freeListTail = pageId

		hp, err := f.pager.GetPageToWrite(txn, HeaderPageID, false)
		if err != nil {
			return err
		}
		defer hp.Release()

		f.setHeader(txn, hp, h, &OpLog{PageID: pageId, Op: OpAdd})

		return nil
	}

	// freed page may not be synced to file just yet. in that case ReadPage returns io.EOF and for the consistence of
	// freelist it needs to be written to disk. Hence, empty bytes are initialized and page is flushed.
	// TODO IMPORTANT: might net be a slotted page
	tp, err := f.pager.GetPageToWrite(txn, h.freeListTail, true)
	if err != nil {
		return err
	}
	defer tp.Release()

	hp, err := f.pager.GetPageToWrite(txn, HeaderPageID, false)
	if err != nil {
		return err
	}
	defer hp.Release()

	h.freeListTail = pageId

	f.setHeader(txn, hp, h, &OpLog{PageID: pageId, Op: OpAdd})
	if err := tp.Set(txn, common.Uint64AsBytes(pageId), &OpLog{PageID: tp.GetPageId(), Op: OpAddSet}); err != nil {
		return err
	}

	return nil
}

// GetHeaderPageLsn returns the latest lsn that modified the header page of the free list. This can be used to
// decide if a page free or alloc log operation has been fully flushed to persistent storage by comparing log's lsn and
// the header page lsn. Since free and alloc operations are committed atomically only after header page is modified,
// if a free page log record has smaller lsn than header page lsn, it is safe to say that operation has been persisted to
// disk and there is no need for a redo.
func (f *List) GetHeaderPageLsn() uint64 {
	// no need to lock because header page is constant
	p, err := f.pager.GetPageToRead(HeaderPageID)
	if err != nil {
		panic(err)
	}

	defer p.Release()

	return p.GetLSN()
}

func (f *List) getHeader() Header {
	if f.header != nil {
		return *f.header
	}

	p, err := f.pager.GetPageToRead(HeaderPageID)
	if err != nil {
		panic(err)
	}

	defer p.Release()

	h := readHeader(p.Get())

	f.header = &h
	return h
}

func (f *List) setHeader(txn transaction.Transaction, hp FreeListPage, h Header, log *OpLog) {
	headerB := make([]byte, 24)
	writeHeader(h, headerB)

	if err := hp.Set(txn, headerB, log); err != nil {
		panic(err)
	}

	f.header = &h
}

func (f *List) initHeader() {
	hp, err := f.pager.GetPageToWrite(transaction.TxnTODO(), HeaderPageID, true)
	if err != nil {
		panic(err)
	}
	defer hp.Release()

	f.setHeader(transaction.TxnTODO(), hp, Header{
		freeListHead: 0,
		freeListTail: 0,
		catalogPID:   0,
	}, &OpLog{Op: OpPop, PageID: HeaderPageID})
}

func readHeader(data []byte) Header {
	return Header{
		freeListHead: binary.BigEndian.Uint64(data),
		freeListTail: binary.BigEndian.Uint64(data[8:]),
		catalogPID:   binary.BigEndian.Uint64(data[16:]),
	}
}

func writeHeader(h Header, dest []byte) {
	binary.BigEndian.PutUint64(dest, h.freeListHead)
	binary.BigEndian.PutUint64(dest[8:], h.freeListTail)
	binary.BigEndian.PutUint64(dest[16:], h.catalogPID)
}
