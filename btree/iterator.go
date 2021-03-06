package btree

import (
	"helin/common"
	"helin/concurrency"
)
/*
	TODO: iterator should take locks
*/

type TreeIterator struct {
	txn     concurrency.Transaction
	tree    *BTree
	curr    Pointer
	currIdx int
	pager   Pager
}

func (it *TreeIterator) Next() (common.Key, interface{}) {
	currNode := it.pager.GetNode(it.curr, Read)
	h := currNode.GetHeader()

	// if there is no element left in node proceed to next node
	if h.KeyLen == int16(it.currIdx) {
		it.pager.Unpin(currNode, false)
		if h.Right == 0 {
			return nil, nil
		}
		it.curr = h.Right
		currNode = it.pager.GetNode(it.curr, Read)
		it.currIdx = 0
	}

	val, key := currNode.GetValueAt(it.currIdx), currNode.GetKeyAt(it.currIdx)
	it.pager.Unpin(currNode, false)
	it.currIdx++
	return key, val
}

func NewTreeIterator(txn concurrency.Transaction, tree *BTree, pager Pager) *TreeIterator {
	curr := tree.GetRoot(Read)
	for !curr.IsLeaf() {
		old := curr
		curr = tree.pager.GetNode(curr.GetValueAt(0).(Pointer), Read)
		tree.pager.Unpin(old, false)
	}

	defer tree.pager.Unpin(curr, false)

	return &TreeIterator{
		txn:     txn,
		tree:    tree,
		curr:    curr.GetPageId(),
		currIdx: 0,
		pager:   pager,
	}
}

func NewTreeIteratorWithKey(txn concurrency.Transaction, key common.Key, tree *BTree, pager Pager) *TreeIterator {
	_, stack := tree.FindAndGetStack(key, Read)
	leaf, idx := stack[len(stack)-1].Node, stack[len(stack)-1].Index
	tree.unpinAll(stack)
	return &TreeIterator{
		txn:     txn,
		tree:    tree,
		curr:    leaf.GetPageId(),
		currIdx: idx,
		pager:   pager,
	}
}
