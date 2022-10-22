package btree

import (
	"helin/common"
	"helin/concurrency"
)

type TreeIterator struct {
	txn      concurrency.Transaction
	tree     *BTree
	curr     Pointer
	currNode Node
	closed   bool
	currIdx  int
	pager    Pager
}

func (it *TreeIterator) Next() (common.Key, interface{}) {
	h := it.currNode.GetHeader()

	// if there is no element left in node proceed to next node
	if h.KeyLen == uint16(it.currIdx) {
		it.pager.Unpin(it.currNode, false)
		it.currNode.RUnLatch()
		if h.Right == 0 {
			it.closed = true
			return nil, nil
		}

		it.curr = h.Right
		it.currNode = it.pager.GetNode(it.curr, Read)
		it.currIdx = 0
	}

	val, key := it.currNode.GetValueAt(it.currIdx), it.currNode.GetKeyAt(it.currIdx)
	it.currIdx++
	return key, val
}

func (it *TreeIterator) Close() error {
	if !it.closed {
		it.pager.Unpin(it.currNode, false)
		it.currNode.RUnLatch()
	}
	return nil
}

func NewTreeIterator(txn concurrency.Transaction, tree *BTree, pager Pager) *TreeIterator {
	curr := tree.GetRoot(Read)
	for !curr.IsLeaf() {
		old := curr
		curr = tree.pager.GetNode(curr.GetValueAt(0).(Pointer), Read)
		tree.pager.Unpin(old, false)
		old.RUnLatch()
	}

	return &TreeIterator{
		txn:      txn,
		tree:     tree,
		curr:     curr.GetPageId(),
		currNode: curr,
		currIdx:  0,
		pager:    pager,
	}
}

func NewTreeIteratorWithKey(txn concurrency.Transaction, key common.Key, tree *BTree, pager Pager) *TreeIterator {
	_, stack := tree.FindAndGetStack(key, Read)
	leaf, idx := stack[len(stack)-1].Node, stack[len(stack)-1].Index
	tree.unpinAll(stack[:len(stack)-1])

	return &TreeIterator{
		txn:      txn,
		tree:     tree,
		curr:     leaf.GetPageId(),
		currNode: leaf,
		currIdx:  idx,
		pager:    pager,
	}
}
