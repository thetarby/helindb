package btree

import (
	"encoding/binary"
	"helin/common"
	"helin/transaction"
)

type Pointer uint64

func (p Pointer) Serialize(dest []byte) {
	binary.BigEndian.PutUint64(dest, uint64(p))
}

func (p Pointer) Bytes() []byte {
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res, uint64(p))
	return res
}

func DeserializePointer(dest []byte) Pointer {
	return Pointer(binary.BigEndian.Uint64(dest))
}

type Keys []common.Key

type NodeIndexPair struct {
	Node  Node
	Index int // pointer index for internal nodes and value index for leaf nodes
}

type TraverseMode int

const (
	Read TraverseMode = iota
	Delete
	Insert
)

type Node interface {
	setKeyAt(txn transaction.Transaction, idx int, key common.Key)
	setValueAt(txn transaction.Transaction, idx int, val interface{})
	GetKeyAt(idx int) common.Key
	GetValueAt(idx int) interface{}
	GetValues() []interface{}
	PrintNode()
	IsOverFlow(degree int) bool
	InsertAt(txn transaction.Transaction, index int, key common.Key, val interface{})
	DeleteAt(txn transaction.Transaction, index int)
	GetPageId() Pointer
	IsLeaf() bool
	GetHeader() *PersistentNodeHeader
	SetHeader(txn transaction.Transaction, h *PersistentNodeHeader)

	// IsSafeForSplit returns true if there is at least one empty place in the node meaning it
	// won't split even one key is inserted
	IsSafeForSplit(degree int) bool

	// IsSafeForMerge returns true if it is more than half full meaning it won't underflow and merge even
	// one key is deleted
	IsSafeForMerge(degree int) bool

	KeyLen() int
	GetRight() Pointer
	IsUnderFlow(degree int) bool

	WLatch()
	WUnlatch()
	RLatch()
	RUnLatch()
}

const (
	PersistentNodeHeaderSize = 3 + 2*NodePointerSize
	NodePointerSize          = 8 // Pointer is int64 which is 8 bytes
)

type PersistentNodeHeader struct {
	IsLeaf uint8
	KeyLen uint16
	Right  Pointer
	Left   Pointer
}

func ReadPersistentNodeHeader(data []byte) *PersistentNodeHeader {
	dest := PersistentNodeHeader{
		IsLeaf: data[0],
		KeyLen: binary.BigEndian.Uint16(data[1:]),
		Right:  Pointer(binary.BigEndian.Uint64(data[3:])),
		Left:   Pointer(binary.BigEndian.Uint64(data[11:])),
	}

	return &dest
}

func WritePersistentNodeHeader(header *PersistentNodeHeader, dest []byte) {
	dest[0] = header.IsLeaf
	binary.BigEndian.PutUint16(dest[1:], header.KeyLen)
	binary.BigEndian.PutUint64(dest[3:], uint64(header.Right))
	binary.BigEndian.PutUint64(dest[11:], uint64(header.Left))
}
