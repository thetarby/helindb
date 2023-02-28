package freelist

import (
	"encoding/binary"
	"errors"
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

type FreeList interface {
	IsIn(pageID uint64) (bool, error)
	Remove(txn transaction.Transaction, pageID uint64) error
	Pop(txn transaction.Transaction) (pageId uint64)
	Add(txn transaction.Transaction, pageId uint64)
	GetHeaderPageLsn() pages.LSN
}

type Header struct {
	freeListHead uint64
	freeListTail uint64
	catalogPID   uint64
}

var _ FreeList = &List{}

type List struct {
	lock   sync.Mutex
	header *Header
	pager  flPager
}

func NewFreeList(txn transaction.Transaction, dm Pager, lm wal.LogManager, init bool) *List {
	list := &List{
		lock: sync.Mutex{},
		pager: flPager{
			bp: dm,
			lm: lm,
		},
	}

	if init {
		list.initHeader(txn)
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

func (f *List) Remove(txn transaction.Transaction, pageID uint64) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	// if list is empty return false
	h := f.getHeader()
	if h.freeListHead == 0 {
		return errors.New("cannot delete from empty free list")
	}

	// if removed page is head, directly use pop operation
	if pageID == h.freeListHead {
		f.Pop(nil)
		return nil
	}

	nextPageID, prevPageID := h.freeListHead, h.freeListHead
	for {
		if nextPageID == pageID {
			// do deletion
			prevPage, err := f.pager.GetPageToWrite(prevPageID)
			if err != nil {
				return err
			}

			nextPage, err := f.pager.GetPageToRead(nextPageID)
			if err != nil {
				return err
			}

			if err := prevPage.SetAt(txn, 0, nextPage.GetAt(0)); err != nil {
				return err
			}
			prevPage.Release()
			nextPage.Release()

			if nextPageID == h.freeListTail {
				h.freeListTail = prevPageID
				f.setHeader(txn, h)
			}

			return nil
		}

		if nextPageID == h.freeListTail {
			break
		}

		// parse next page id
		p, err := f.pager.GetPageToRead(h.freeListHead)
		if err != nil {
			panic(err)
		}

		nextPageID = binary.BigEndian.Uint64(p.GetAt(0))
		p.Release()
	}

	return errors.New("page cannot be found in free list")
}

func (f *List) Pop(txn transaction.Transaction) (pageId uint64) {
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
		f.setHeader(txn, h)
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

	f.setHeader(txn, h)
	return
}

func (f *List) Add(txn transaction.Transaction, pageId uint64) {
	f.lock.Lock()
	defer f.lock.Unlock()

	h := f.getHeader()

	// if free list is empty
	if h.freeListHead == 0 {
		h.freeListHead = pageId
		h.freeListTail = pageId
		f.setHeader(txn, h)
		return
	}

	// freed page may not be synced to file just yet. in that case ReadPage returns io.EOF and for the consistence of
	// freelist it needs to be written to disk. Hence, empty bytes are initialized and page is flushed.
	p, err := f.pager.GetPageToWrite(h.freeListTail)
	if err != nil {
		panic(err)
	}

	if err := p.SetAt(txn, 0, common.Uint64AsBytes(pageId)); err != nil {
		panic(err)
	}
	p.Release()

	h.freeListTail = pageId
	f.setHeader(txn, h)
}

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

func (f *List) setHeader(txn transaction.Transaction, h Header) {
	f.header = &h

	p, err := f.pager.GetPageToWrite(HeaderPageID)
	if err != nil {
		panic(err)
	}

	headerB := make([]byte, 24)
	writeHeader(h, headerB)
	if err := p.SetAt(txn, 0, headerB); err != nil {
		panic(err)
	}
	p.Release()
}

func (f *List) initHeader(txn transaction.Transaction) {
	f.setHeader(txn, Header{
		freeListHead: 0,
		freeListTail: 0,
		catalogPID:   0,
	})
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
