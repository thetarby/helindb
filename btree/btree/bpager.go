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
}

type BPageReleaser interface {
	BPage
	Release()
}

// BPager is the interface that is responsible for creating, fetching and freeing BPage instances.
// It also provides methods to manage overflow.
type BPager interface {
	// NewBPage creates a new page, puts it into a buffer, pins it and acquires an exclusive latch on it
	NewBPage(txn transaction.Transaction) (BPageReleaser, error)

	// GetBPageToRead reads the page to a buffer and pins it
	GetBPageToRead(txn transaction.Transaction, p Pointer) (BPageReleaser, error)

	// GetBPageToWrite reads the page to a buffer and pins it
	GetBPageToWrite(txn transaction.Transaction, p Pointer) (BPageReleaser, error)

	// Unpin decreases pin count of the page
	Unpin(p Pointer)

	// FreeBPage frees page and is used when page is no longer used
	FreeBPage(txn transaction.Transaction, p Pointer)

	CreateOverflow(txn transaction.Transaction) (OverflowReleaser, error)
	FreeOverflow(txn transaction.Transaction, p Pointer) error
	GetOverflowReleaser(p Pointer) (OverflowReleaser, error)
}

// Overflow is the interface that is used to store overflowed parts of a node's payload.
type Overflow interface {
	GetPageId() uint64
	GetAt(txn transaction.Transaction, idx int) ([]byte, error)
	Insert(txn transaction.Transaction, data []byte) (int, error)
	SetAt(txn transaction.Transaction, idx int, data []byte) error
	DeleteAt(txn transaction.Transaction, idx int) error
	Count(txn transaction.Transaction) (int, error)
	Free(txn transaction.Transaction) error
}

type OverflowReleaser interface {
	Overflow
}
