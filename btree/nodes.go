package btree

import (
	"sort"
)

type Pointer int64

type Key interface {
	Less(than Key) bool
}

type Keys []Key

func (k Keys) find(item Key) (index int, found bool) {
	i := sort.Search(len(k), func(i int) bool {
		return item.Less(k[i])
	})
	if i > 0 && !k[i-1].Less(item) {
		return i - 1, true
	}
	return i, false
}

type NodeIndexPair struct {
	Node  Pointer
	Index int // pointer index for internal nodes and value index for leaf nodes
}

type TraverseMode int

const (
    Read TraverseMode = iota
    Delete
    Insert
)

type Node interface {
	// findAndGetStack is used to recursively find the given key and it also passes a stack object recursively to
	// keep the path it followed down to leaf node. value is nil when key does not exist.
	findAndGetStack(key Key, stackIn []NodeIndexPair, mode TraverseMode) (value interface{}, stackOut []NodeIndexPair)
	findKey(key Key) (index int, found bool)
	shiftKeyValueToRightAt(n int)
	shiftKeyValueToLeftAt(n int)
	setKeyAt(idx int, key Key)
	setValueAt(idx int, val interface{})
	GetKeyAt(idx int) Key
	GetValueAt(idx int) interface{}
	GetValues() []interface{}
	SplitNode(index int) (right Pointer, keyAtLeft Key, keyAtRight Key)
	PrintNode()
	IsOverFlow(degree int) bool
	InsertAt(index int, key Key, val interface{})
	DeleteAt(index int)
	GetPageId() Pointer
	IsLeaf() bool
	
	// IsSafeForSplit returns true if there is at least one empty place in the node meaning it 
	// won't split even one key is inserted
	IsSafeForSplit(degree int) bool
	
	// IsSafeForMerge returns true if it is more than half full meaning it won't underflow and merge even 
	// one key is deleted
	IsSafeForMerge(degree int) bool

	/* delete related methods */

	Keylen() int
	GetRight() Pointer
	MergeNodes(rightNode Node, parent Node)
	Redistribute(rightNode_ Node, parent_ Node)
	IsUnderFlow(degree int) bool
}