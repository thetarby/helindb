package btree

import (
	"helin/transaction"
)

// BPage is the minimal interface that btree nodes uses.
type BPage interface {
	GetAt(idx int) []byte
	InsertAt(txn transaction.Transaction, idx int, data []byte) error
	SetAt(txn transaction.Transaction, idx int, data []byte) error
	DeleteAt(txn transaction.Transaction, idx int) error

	EmptySpace() int
	Cap() int
	Count() uint16

	GetPageId() Pointer

	WLatch()
	WUnlatch()
	RLatch()
	RUnLatch()
}

// BPager is the interface that is responsible for creating, fetching and freeing BPage instances.
// It also provides methods to manage overflow.
type BPager interface {
	// NewBPage creates a new page, puts it into a buffer, pins it and acquires an exclusive latch on it
	NewBPage(txn transaction.Transaction) BPage

	// GetBPage reads the page to a buffer and pins it, acquiring necessary latches is the responsibility of the caller
	GetBPage(p Pointer) (BPage, error)

	// Unpin decreases pin count of the page
	Unpin(p Pointer)

	// FreeBPage frees page and is used when page is no longer used
	FreeBPage(txn transaction.Transaction, p Pointer)

	CreateOverflow(txn transaction.Transaction) OverflowReleaser
	FreeOverflow(txn transaction.Transaction, p Pointer)
	GetOverflowReleaser(p Pointer) OverflowReleaser
}

// Overflow is the interface that is used to store overflowed parts of a node's payload.
type Overflow interface {
	GetPageId() uint64
	GetAt(idx int) ([]byte, error)
	Insert(txn transaction.Transaction, data []byte) (int, error)
	SetAt(txn transaction.Transaction, idx int, data []byte) error
	DeleteAt(txn transaction.Transaction, idx int) error
	Count() (int, error)
	Free(txn transaction.Transaction) error
}

type OverflowReleaser interface {
	Overflow
}

type NodeReleaser interface {
	node
	Release()
}

type BPageReleaser interface {
	BPage
	Release()
}
