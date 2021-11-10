package btree

import (
	"fmt"
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

type Node interface {
	// findAndGetStack is used to recursively find the given key and it also passes a stack object recursively to
	// keep the path it followed down to leaf node. value is nil when key does not exist.
	findAndGetStack(key Key, stackIn []NodeIndexPair) (value interface{}, stackOut []NodeIndexPair)
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

	/* delete related methods */

	Keylen() int
	GetRight() Pointer
	GetLeft() Pointer
	MergeNodes(rightNode Node, parent Node)
	Redistribute(rightNode_ Node, parent_ Node)
	IsUnderFlow(degree int) bool
}

type InternalNode struct {
	PersistentPage
	Keys     Keys
	Pointers []Pointer
	pager    Pager
}

type LeafNode struct {
	PersistentPage
	Keys   Keys
	Values []interface{}
	Right  *LeafNode
	Left   *LeafNode
	pager  Pager
}

func newInternalNode(firstPointer Pointer) (n *InternalNode) {
	// TODO: do not forget to add first pointer
	pager := n.pager
	return pager.NewInternalNode(firstPointer).(*InternalNode)
}

func newLeafNode() (n *LeafNode) {
	pager := n.pager
	return pager.NewLeafNode().(*LeafNode)
}

func (n *LeafNode) IsLeaf() bool {
	return true
}

func (n *InternalNode) IsLeaf() bool {
	return false
}

func (n *LeafNode) findKey(key Key) (index int, found bool) {
	return n.Keys.find(key)
}

func (n *InternalNode) findKey(key Key) (index int, found bool) {
	return n.Keys.find(key)
}

func (n *LeafNode) shiftKeyValueToRightAt(at int) {
	n.Keys = append(n.Keys, nil)
	n.Values = append(n.Values, nil)
	copy(n.Keys[at+1:], n.Keys[at:])
	copy(n.Values[at+1:], n.Values[at:])
}

func (n *InternalNode) shiftKeyValueToRightAt(at int) {
	n.Keys = append(n.Keys, nil)
	var zeroPointer Pointer
	n.Pointers = append(n.Pointers, zeroPointer)
	copy(n.Keys[at+1:], n.Keys[at:])
	copy(n.Pointers[at+2:], n.Pointers[at+1:])
}

func (n *LeafNode) shiftKeyValueToLeftAt(at int) {
	panic("implement me")
}

func (n *InternalNode) shiftKeyValueToLeftAt(at int) {
	panic("implement me")
}

func (n *LeafNode) GetKeyAt(idx int) Key {
	return n.Keys[idx]
}

func (n *InternalNode) GetKeyAt(idx int) Key {
	return n.Keys[idx]
}

func (n *LeafNode) GetValueAt(idx int) interface{} {
	return n.Values[idx]
}

func (n *InternalNode) GetValueAt(idx int) interface{} {
	return n.Pointers[idx]
}

func (n *LeafNode) GetValues() []interface{} {
	return n.Values
}

func (n *InternalNode) GetValues() []interface{} {
	res := make([]interface{}, 0)
	for i := 0; i < len(n.Pointers); i++ {
		res = append(res, n.Pointers[i])
	}
	return res
}

func (n *LeafNode) setKeyAt(idx int, key Key) {
	n.Keys[idx] = key
}

func (n *InternalNode) setKeyAt(idx int, key Key) {
	n.Keys[idx] = key
}

func (n *LeafNode) setValueAt(idx int, val interface{}) {
	n.Values[idx] = val
}

func (n *InternalNode) setValueAt(idx int, val interface{}) {
	n.Pointers[idx] = val.(Pointer)
}

func (n *LeafNode) IsOverFlow(degree int) bool {
	return len(n.Values) == degree
}

func (n *InternalNode) IsOverFlow(degree int) bool {
	return len(n.Pointers) == degree+1
}

func (n *LeafNode) InsertAt(index int, key Key, value interface{}) {
	n.shiftKeyValueToRightAt(index)
	n.setKeyAt(index, key)
	n.setValueAt(index, value)
}

func (n *InternalNode) InsertAt(index int, key Key, pointer interface{}) {
	n.shiftKeyValueToRightAt(index)
	n.setKeyAt(index, key)
	n.setValueAt(index+1, pointer)
}

func (n *LeafNode) SplitNode(index int) (rightNode Pointer, keyAtLeft Key, keyAtRight Key) {
	right := n.pager.NewLeafNode().(*LeafNode)
	keyAtLeft = n.Keys[index-1]
	keyAtRight = n.Keys[index]

	right.Keys = append(right.Keys, n.Keys[index:]...)
	right.Values = append(right.Values, n.Values[index:]...)
	n.Keys = n.Keys[:index]
	n.Values = n.Values[:index]
	right.Right = n.Right
	n.Right = right
	right.Left = n

	return right.GetPageId(), keyAtLeft, keyAtRight
}

func (n *InternalNode) SplitNode(index int) (rightNode Pointer, keyAtLeft Key, keyAtRight Key) {
	right := n.pager.NewInternalNode(n.Pointers[index+1]).(*InternalNode)
	keyAtLeft = n.Keys[index-1]
	keyAtRight = n.Keys[index]

	right.Keys = append(right.Keys, n.Keys[index+1:]...)
	if len(n.Pointers) > index+2 {
		right.Pointers = append(right.Pointers, n.Pointers[index+2:]...)
	}

	n.Keys = n.Keys[:index]
	n.Pointers = n.Pointers[:index]

	n.truncate(index)

	return right.GetPageId(), keyAtLeft, keyAtRight
}

func (n *LeafNode) findAndGetStack(key Key, stackIn []NodeIndexPair) (value interface{}, stackOut []NodeIndexPair) {
	i, found := n.findKey(key)
	stackOut = append(stackIn, NodeIndexPair{n.GetPageId(), i})
	if !found {
		return nil, stackOut
	}
	return n.Values[i], stackOut
}

func (n *InternalNode) findAndGetStack(key Key, stackIn []NodeIndexPair) (value interface{}, stackOut []NodeIndexPair) {
	pager := n.pager
	i, found := n.findKey(key)
	if found {
		i++
	}
	stackOut = append(stackIn, NodeIndexPair{n.GetPageId(), i})
	node := pager.GetNode(n.Pointers[i])
	res, stackOut := node.findAndGetStack(key, stackOut)
	return res, stackOut
}

func (n *LeafNode) PrintNode() {
	fmt.Printf("Node( ")
	for i := 0; i < len(n.Keys); i++ {
		fmt.Printf("%v | ", n.Keys[i])
	}
	fmt.Printf(")    ")
}

func (n *InternalNode) PrintNode() {
	fmt.Printf("Node( ")
	for i := 0; i < len(n.Keys); i++ {
		fmt.Printf("%v | ", n.Keys[i])
	}
	fmt.Printf(")    ")
}
