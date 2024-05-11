package freelist

import (
	"encoding/binary"
	"helin/common"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
	"sync"
)

const (
	HeaderPageID = uint64(1)
)

type Pager interface {
	GetPage(pageId uint64) (*pages.RawPage, error)
	Unpin(pageId uint64, isDirty bool) bool
}

/*
	Freelist operations are atomic. Pop and Add operations appends page free and alloc log records. If header page lsn
	is greater than any page free or alloc log record's lsn, it can safely be considered fully committed. Otherwise,
	it is not committed.
*/

type FreeList interface {
	IsIn(pageID uint64) (bool, error)
	Pop(txn transaction.TxnID) (pageId uint64)
	Add(txn transaction.TxnID, pageId uint64)
	AddInRecovery(txn transaction.TxnID, pageId uint64, undoNext pages.LSN)
	GetHeaderPageLsn() pages.LSN
}

type Header struct {
	freeListHead uint64
	freeListTail uint64
	catalogPID   uint64
}

var _ FreeList = &List{}

type List struct {
	lock          sync.Mutex
	header        *Header
	pager         flPager
	log           wal.LogManager
	enableLogging bool
}

func NewFreeList(dm Pager, lm wal.LogManager, init bool) *List {
	list := &List{
		lock: sync.Mutex{},
		pager: flPager{
			bp: dm,
			lm: lm,
		},
		log: lm,
	}

	if init {
		list.initHeader()
	}
	return list
}

func (f *List) IsIn(pageID uint64) (bool, error) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// if list is empty return false
	h := f.getHeader()
	if h.freeListHead == 0 {
		return false, nil
	}

	nextPageID := h.freeListHead
	for {
		if nextPageID == pageID {
			return true, nil
		}

		if nextPageID == h.freeListTail {
			return false, nil
		}

		// parse next page id
		p, err := f.pager.GetPageToRead(h.freeListHead)
		if err != nil {
			return false, err
		}

		nextPageID = binary.BigEndian.Uint64(p.GetAt(0))
		p.Release()
	}
}

// Pop pops a page from free list. Unlike Add, this operation can be undone by simply adding page back to free list.
func (f *List) Pop(txn transaction.TxnID) (pageId uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	// if list is empty return 0
	h := f.getHeader()
	if h.freeListHead == 0 {
		return 0
	}

	// if there is only on entry in free list return that and set head and tail to 0
	if h.freeListHead == h.freeListTail {
		pageId = h.freeListHead
		h.freeListHead, h.freeListTail = 0, 0

		f.setHeader(txn, pageId, h, false, pages.ZeroLSN)
		return
	}

	// else pop head, read new head and update header
	pageId = h.freeListHead

	p, err := f.pager.GetPageToRead(h.freeListHead)
	if err != nil {
		panic(err)
	}

	h.freeListHead = binary.BigEndian.Uint64(p.GetAt(0))
	p.Release()

	f.setHeader(txn, pageId, h, false, pages.ZeroLSN)
	return
}

// Add adds page to free list. Note that this operation converts pages to SlottedPage and conversion operations are
// not logged, meaning this operation cannot be undone. Transactions should only free pages after they are committed
// so that page free operations are never rolled back.
func (f *List) Add(txn transaction.TxnID, pageId uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	h := f.getHeader()

	// if free list is empty
	if h.freeListHead == 0 {
		h.freeListHead = pageId
		h.freeListTail = pageId
		f.setHeader(txn, pageId, h, true, pages.ZeroLSN)
		return
	}

	// freed page may not be synced to file just yet. in that case ReadPage returns io.EOF and for the consistence of
	// freelist it needs to be written to disk. Hence, empty bytes are initialized and page is flushed.
	p, err := f.pager.GetPageToWrite(h.freeListTail)
	if err != nil {
		panic(err)
	}

	// TODO: what about this?
	if err := p.SetAt(txn, 0, common.Uint64AsBytes(pageId)); err != nil {
		panic(err)
	}
	p.Release()

	h.freeListTail = pageId

	f.setHeader(txn, pageId, h, true, pages.ZeroLSN)
}

func (f *List) AddInRecovery(txn transaction.TxnID, pageId uint64, undoNext pages.LSN) {
	f.lock.Lock()
	defer f.lock.Unlock()

	h := f.getHeader()

	// if free list is empty
	if h.freeListHead == 0 {
		h.freeListHead = pageId
		h.freeListTail = pageId
		f.setHeader(txn, pageId, h, true, undoNext)
		return
	}

	// freed page may not be synced to file just yet. in that case ReadPage returns io.EOF and for the consistence of
	// freelist it needs to be written to disk. Hence, empty bytes are initialized and page is flushed.
	p, err := f.pager.GetPageToWrite(h.freeListTail)
	if err != nil {
		panic(err)
	}

	// TODO: what about this?
	if err := p.SetAt(txn, 0, common.Uint64AsBytes(pageId)); err != nil {
		panic(err)
	}
	p.Release()

	h.freeListTail = pageId

	f.setHeader(txn, pageId, h, true, undoNext)
}

// GetHeaderPageLsn returns the latest lsn that modified the header page of the free list. This can be used to
// decide if a page free or alloc log operation has been fully flushed to persistent storage by comparing log's lsn and
// the header page lsn. Since free and alloc operations are committed atomically only after header page is modified,
// if a free page log record has smaller lsn than header page lsn, it is safe to say that operation has been persisted to
// disk and there is no need for a redo.
func (f *List) GetHeaderPageLsn() pages.LSN {
	// no need to lock because header page is constant
	p, err := f.pager.GetPageToRead(HeaderPageID)
	if err != nil {
		panic(err)
	}

	p.RLatch()
	defer p.RUnLatch()

	return p.GetPageLSN()
}

func (f *List) getHeader() Header {
	if f.header != nil {
		return *f.header
	}

	p, err := f.pager.GetPageToRead(HeaderPageID)
	if err != nil {
		panic(err)
	}

	h := ReadHeader(p.GetAt(0))
	p.Release()

	f.header = &h
	return h
}

func (f *List) setHeader(txn transaction.TxnID, pageID uint64, h Header, isFree bool, undoNext pages.LSN) {
	f.header = &h

	p, err := f.pager.GetPageToWrite(HeaderPageID)
	if err != nil {
		panic(err)
	}

	headerB := make([]byte, 24)
	writeHeader(h, headerB)
	if isFree {
		if err := p.SetAtFree(txn, pageID, 0, headerB, undoNext); err != nil {
			panic(err)
		}
	} else {
		if err := p.SetAtAlloc(txn, pageID, 0, headerB, undoNext); err != nil {
			panic(err)
		}
	}
	defer p.Release()
}

func (f *List) initHeader() {
	f.setHeader(transaction.TxnTODO().GetID(), 0, Header{
		freeListHead: 0,
		freeListTail: 0,
		catalogPID:   0,
	}, false, pages.ZeroLSN)
}

func (f *List) appendLog(record *wal.LogRecord) pages.LSN {
	if f.enableLogging {
		return f.log.AppendLog(record)
	}

	return pages.ZeroLSN
}

func ReadHeader(data []byte) Header {
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
