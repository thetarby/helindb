package btree

import (
	"helin/common"
	"helin/transaction"
)

type TreeIterator struct {
	txn      transaction.Transaction
	tree     *BTree
	curr     Pointer
	currNode nodeReleaser
	closed   bool
	currIdx  int
	pager    *Pager2
}

func (it *TreeIterator) Next() (common.Key, any) {
	// if there is no element left in node proceed to next node
	for it.currNode.KeyLen() == (it.currIdx) {
		it.currNode.Release()
		if it.currNode.GetRight() == 0 {
			it.closed = true
			return nil, nil
		}

		it.curr = it.currNode.GetRight()
		it.currNode = it.pager.GetNodeReleaser(it.txn, it.curr, Read)
		it.currIdx = 0
	}

	val, key := it.currNode.GetValueAt(it.txn, it.currIdx), it.currNode.GetKeyAt(it.txn, it.currIdx)
	it.currIdx++
	return key, val
}

func (it *TreeIterator) Close() error {
	if !it.closed {
		it.currNode.Release()
	}
	return nil
}

func NewTreeIterator(txn transaction.Transaction, tree *BTree) *TreeIterator {
	curr := tree.GetRoot(txn, Read)
	for !curr.IsLeaf() {
		old := curr
		curr = tree.pager.GetNodeReleaser(txn, curr.GetValueAt(txn, 0).(Pointer), Read)
		old.Release()
	}

	return &TreeIterator{
		txn:      txn,
		tree:     tree,
		curr:     curr.GetPageId(),
		currNode: curr,
		currIdx:  0,
		pager:    tree.pager,
	}
}

func NewTreeIteratorWithKey(txn transaction.Transaction, key common.Key, tree *BTree) *TreeIterator {
	_, stack := tree.FindAndGetStack(txn, key, Read)
	leaf, idx := stack[len(stack)-1].Node, stack[len(stack)-1].Index

	return &TreeIterator{
		txn:      txn,
		tree:     tree,
		curr:     leaf.GetPageId(),
		currNode: leaf,
		currIdx:  idx,
		pager:    tree.pager,
	}
}
