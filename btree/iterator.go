package btree

import (
	"helin/concurrency"
)

type TreeIterator struct {
	txn     concurrency.Transaction
	tree    *BTree
	curr    Pointer
	currIdx int
	pager   *BufferPoolPager
}

func (it *TreeIterator) Next() interface{} {
	currNode := it.pager.GetNode(it.curr)
	h := currNode.GetHeader()

	// if there is no element left in node proceed to next node
	if h.KeyLen == int16(it.currIdx) {
		it.pager.Unpin(currNode, false)
		if h.Right == 0 {
			return nil
		}
		it.curr = h.Right
		currNode = it.pager.GetNode(it.curr)
		it.currIdx = 0
	}

	val := currNode.GetValueAt(it.currIdx)
	it.pager.Unpin(currNode, false)
	it.currIdx++
	return val
}

func NewTreeIterator(txn concurrency.Transaction, tree *BTree, pager *BufferPoolPager) *TreeIterator {
	return &TreeIterator{
		txn:     txn,
		tree:    tree,
		curr:    tree.Root,
		currIdx: 0,
		pager:   pager,
	}
}

func NewTreeIteratorWithKey(txn concurrency.Transaction, key Key, tree *BTree, pager *BufferPoolPager) *TreeIterator {
	_, stack := tree.FindAndGetStack(key, Read)
	leaf, idx := stack[len(stack)-1].Node,  stack[len(stack)-1].Index
	
	return &TreeIterator{
		txn:     txn,
		tree:    tree,
		curr:    leaf,
		currIdx: idx,
		pager:   pager,
	}
}