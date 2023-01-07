package btree

import (
	"helin/transaction"
)

type NodePage interface {
	GetData() []byte
	GetPageId() Pointer
	WLatch()
	WUnlatch()
	RLatch()
	RUnLatch()

	GetAt(idx int) []byte
	InsertAt(txn transaction.Transaction, idx int, data []byte) error
	SetAt(txn transaction.Transaction, idx int, data []byte) error
	DeleteAt(txn transaction.Transaction, idx int) error
}

type Pager interface {
	// NewInternalNode first should create a PersistentPage which points to a byte array.
	// Then initialize an InternalNode structure.
	// Finally, it should serialize the structure on to pointed byte array.
	// NOTE: the node should have a reference(by extending it for example) to the created PersistentPage
	// so that it can be serialized in the future when its state changes.
	// NOTE: takes write latch on created node, caller should release it
	NewInternalNode(txn transaction.Transaction, p Pointer) Node

	// NewLeafNode first should create an PersistentPage which points to a byte array.
	// Then initialize a LeafNode structure.
	// Finally, it should serialize the structure on to pointed byte array
	// NOTE: takes write latch on created node, caller should release it
	NewLeafNode(txn transaction.Transaction) Node

	// GetNode returns a Node given a Pointer. Should be able to deserialize a node from byte arr and should be able to
	// recognize if it is an InternalNode or LeafNode and return the correct type.
	// NOTE: If TraverseMode is read returned node is read latched otherwise it is write-latched and caller should also
	// release latches when Node is not needed anymore.
	GetNode(p Pointer, mode TraverseMode) Node

	// Unpin and UnpinByPointer methods are useful when underlying pager is a persistent one.
	// For an in memory implementation these methods can be noop.
	Unpin(n Node, isDirty bool)

	Free(txn transaction.Transaction, p Pointer) error
	FreeNode(txn transaction.Transaction, n Node) error

	UnpinByPointer(p Pointer, isDirty bool)

	CreatePage(txn transaction.Transaction) NodePage
	GetPage(p Pointer) NodePage
}
