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

type NodeIndexPair struct {
	Node  nodeReleaser
	Index int // pointer index for internal nodes and value index for leaf nodes
}

type TraverseMode int

const (
	Read TraverseMode = iota
	Delete
	Insert
	Debug
)

// node is an internal interface which is not exported. it is only for readability purposes.
type node interface {
	SetKeyAt(txn transaction.Transaction, idx int, key common.Key) error
	SetValueAt(txn transaction.Transaction, idx int, val any) error
	GetKeyAt(txn transaction.Transaction, idx int) (common.Key, error)
	GetValueAt(txn transaction.Transaction, idx int) (any, error)
	GetValues(txn transaction.Transaction) ([]any, error)
	InsertAt(txn transaction.Transaction, index int, key common.Key, val any) error
	DeleteAt(txn transaction.Transaction, index int) error

	GetPageId() Pointer
	IsLeaf() bool
	GetHeader() *PersistentNodeHeader
	SetHeader(txn transaction.Transaction, h *PersistentNodeHeader)
	PrintNode(txn transaction.Transaction)

	KeyLen() int
	FillFactor() int
	GetRight() Pointer
}

type nodeReleaser interface {
	node
	Release()
}

const (
	PersistentNodeHeaderSize = 3 + 3*NodePointerSize
	NodePointerSize          = 8 // Pointer is int64 which is 8 bytes
)

type PersistentNodeHeader struct {
	IsLeaf   uint8
	KeyLen   uint16
	Right    Pointer
	Left     Pointer
	Overflow Pointer
}

func ReadPersistentNodeHeader(data []byte) *PersistentNodeHeader {
	dest := PersistentNodeHeader{
		IsLeaf:   data[0],
		KeyLen:   binary.BigEndian.Uint16(data[1:]),
		Right:    Pointer(binary.BigEndian.Uint64(data[3:])),
		Left:     Pointer(binary.BigEndian.Uint64(data[11:])),
		Overflow: Pointer(binary.BigEndian.Uint64(data[19:])),
	}

	return &dest
}

func WritePersistentNodeHeader(header *PersistentNodeHeader, dest []byte) {
	dest[0] = header.IsLeaf
	binary.BigEndian.PutUint16(dest[1:], header.KeyLen)
	binary.BigEndian.PutUint64(dest[3:], uint64(header.Right))
	binary.BigEndian.PutUint64(dest[11:], uint64(header.Left))
	binary.BigEndian.PutUint64(dest[19:], uint64(header.Overflow))
}
