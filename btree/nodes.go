package btree

import (
	"encoding/binary"
	"helin/common"
)

type Pointer uint64

func (p Pointer) Serialize(dest []byte){
	binary.BigEndian.PutUint64(dest, uint64(p))
}

func (p Pointer) Bytes() []byte{
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res, uint64(p))
	return res
}

func DeserializePointer(dest []byte) Pointer{
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
	setKeyAt(idx int, key common.Key)
	setValueAt(idx int, val interface{})
	GetKeyAt(idx int) common.Key
	GetValueAt(idx int) interface{}
	GetValues() []interface{}
	PrintNode()
	IsOverFlow(degree int) bool
	InsertAt(index int, key common.Key, val interface{})
	DeleteAt(index int)
	GetPageId() Pointer
	IsLeaf() bool
	GetHeader() *PersistentNodeHeader
	SetHeader(*PersistentNodeHeader)

	// IsSafeForSplit returns true if there is at least one empty place in the node meaning it
	// won't split even one key is inserted
	IsSafeForSplit(degree int) bool

	// IsSafeForMerge returns true if it is more than half full meaning it won't underflow and merge even
	// one key is deleted
	IsSafeForMerge(degree int) bool

	Keylen() int
	GetRight() Pointer
	IsUnderFlow(degree int) bool

	WLatch()
	WUnlatch()
	RLatch()
	RUnLatch()
}
