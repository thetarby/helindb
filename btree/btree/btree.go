package btree

import (
	"encoding/binary"
	"fmt"
	"helin/common"
	"helin/transaction"
	"log"
	"sort"
	"sync"
)

/*
	during re-balancing after delete if parent nodes are involved parents might have overflow when copied up key is
	longer than the original.
*/

type BTree struct {
	// degree is actually stored in metaPage, but it is cached here. since it is a constant value it is safe
	// to set it once when opening btree and use until closing.
	degree int // TODO: make this uint8

	maxThatCouldBeFit  int
	overFlowThreshold  int
	underFlowThreshold int
	disMax             int

	// metaPID is the page id of the metadata page of btree. it cannot change and is used to store
	// information such as tree's root node pointer, degree etc... db schema must keep track of metaPID
	// to reconstruct btree after db is closed and reopened.
	metaPID Pointer

	pager         *Pager2
	rootEntryLock *sync.RWMutex
}

// metaPage is a wrapper around BPage that is used to store btree meta info.
type metaPage struct {
	BPageReleaser
}

func (m *metaPage) getRoot() Pointer {
	return Pointer(binary.BigEndian.Uint64(m.GetAt(0)))
}

func (m *metaPage) setRoot(txn transaction.Transaction, p Pointer) {
	err := m.SetAt(txn, 0, p.Bytes())
	CheckErr(err)
}

func (m *metaPage) getDegree() int {
	return int(binary.BigEndian.Uint16(m.GetAt(1)))
}

func (m *metaPage) setDegree(txn transaction.Transaction, degree int) {
	dest := make([]byte, 2)
	binary.BigEndian.PutUint16(dest, uint16(degree))
	err := m.SetAt(txn, 1, dest)
	CheckErr(err)
}

func NewBtreeWithPager(txn transaction.Transaction, degree int, pager *Pager2) *BTree {
	mp, err := pager.CreatePage(txn)
	CheckErr(err)
	meta := metaPage{mp}
	defer meta.Release()

	l, err := pager.NewLeafNode(txn)
	CheckErr(err)
	defer l.Release()

	root, err := pager.NewInternalNode(txn, l.GetPageId())
	CheckErr(err)
	defer root.Release()

	meta.setRoot(txn, root.GetPageId())
	meta.setDegree(txn, degree)

	maxThatCouldBeFit := meta.getDegree()
	overFlowThreshold := maxThatCouldBeFit - 1
	underFlowThreshold := overFlowThreshold / 2
	disMax := overFlowThreshold - underFlowThreshold

	return &BTree{
		degree:             degree,
		maxThatCouldBeFit:  maxThatCouldBeFit,
		overFlowThreshold:  overFlowThreshold,
		underFlowThreshold: underFlowThreshold,
		disMax:             disMax,
		pager:              pager,
		rootEntryLock:      &sync.RWMutex{},
		metaPID:            meta.GetPageId(),
	}
}

func ConstructBtreeByMeta(txn transaction.Transaction, metaPID Pointer, pager *Pager2) *BTree {
	meta := metaPage{pager.GetPage(txn, metaPID, true)}
	defer meta.Release()

	maxThatCouldBeFit := meta.getDegree()
	overFlowThreshold := maxThatCouldBeFit - 1
	underFlowThreshold := overFlowThreshold / 2
	disMax := overFlowThreshold - underFlowThreshold

	return &BTree{
		degree:             meta.getDegree(),
		maxThatCouldBeFit:  maxThatCouldBeFit,
		overFlowThreshold:  overFlowThreshold,
		underFlowThreshold: underFlowThreshold,
		disMax:             disMax,
		metaPID:            metaPID,
		pager:              pager,
		rootEntryLock:      &sync.RWMutex{},
	}
}

func (tree *BTree) GetMetaPID() Pointer {
	return tree.metaPID
}

func (tree *BTree) GetRoot(txn transaction.Transaction, mode TraverseMode) nodeReleaser {
	return tree.pager.GetNodeReleaser(txn, tree.getRoot(txn), mode)
}

func (tree *BTree) meta(txn transaction.Transaction, readOnly bool) *metaPage {
	meta := tree.pager.GetPage(txn, tree.metaPID, readOnly)
	return &metaPage{meta}
}

func (tree *BTree) getRoot(txn transaction.Transaction) Pointer {
	meta := tree.meta(txn, true)
	defer meta.Release()

	return meta.getRoot()
}

func (tree *BTree) setRoot(txn transaction.Transaction, p Pointer) {
	meta := tree.meta(txn, false)
	defer meta.Release()
	meta.setRoot(txn, p)
}

func (tree *BTree) GetPager() *Pager2 {
	return tree.pager
}

func (tree *BTree) Insert(txn transaction.Transaction, key common.Key, value any) {
	i, stack := tree.FindAndGetStack(txn, key, Insert)
	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1 {
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	if i != nil {
		panic(fmt.Sprintf("key already exists:  %v", key))
	}
	defer func() { release(stack) }()

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		stack = stack[:len(stack)-1]
		i, _ := tree.FindKey(txn, popped, key)
		popped.InsertAt(txn, i, rightKey, rightNod)

		if tree.isOverFlow(popped) {
			rightNod, _, rightKey = tree.splitNode(txn, popped)
			if rootLocked && popped.GetPageId() == tree.getRoot(txn) {
				leftNode := popped

				newRoot, err := tree.pager.NewInternalNode(txn, leftNode.GetPageId())
				CheckErr(err)

				newRoot.InsertAt(txn, 0, rightKey, rightNod.(Pointer))
				tree.setRoot(txn, newRoot.GetPageId())

				newRoot.Release()
			}

			popped.Release()
		} else {
			popped.Release()
			break
		}
	}
}

func (tree *BTree) Set(txn transaction.Transaction, key common.Key, value any) (isInserted bool) {
	i, stack := tree.FindAndGetStack(txn, key, Insert)
	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1 {
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	defer func() { release(stack) }()

	if i != nil {
		// top of stack is the leaf node
		topOfStack := stack[len(stack)-1]
		leafNode := topOfStack.Node
		leafNode.SetValueAt(txn, topOfStack.Index, value)
		stack = stack[:len(stack)-1]
		topOfStack.Node.Release()
		return false
	}

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		stack = stack[:len(stack)-1]
		i, _ := tree.FindKey(txn, popped, key)
		popped.InsertAt(txn, i, rightKey, rightNod)

		if tree.isOverFlow(popped) {
			rightNod, _, rightKey = tree.splitNode(txn, popped)
			popped.Release()
			if rootLocked && popped.GetPageId() == tree.getRoot(txn) {
				leftNode := popped

				newRoot, err := tree.pager.NewInternalNode(txn, leftNode.GetPageId())
				CheckErr(err)

				newRoot.InsertAt(txn, 0, rightKey, rightNod.(Pointer))
				tree.setRoot(txn, newRoot.GetPageId())
				newRoot.Release()
			}
		} else {
			popped.Release()
			break
		}
	}

	return true
}

func (tree *BTree) Delete(txn transaction.Transaction, key common.Key) bool {
	i, stack := tree.FindAndGetStack(txn, key, Delete)
	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1 {
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	defer func() { release(stack) }()
	if i == nil {
		return false
	}

	// IMPORTANT NOTE: freeing pages should be delayed because if txn fails, during rollback, recovery should allocate the exact
	// same page because there might be pointers pointing to it. If pages are freed directly another txn can allocate
	// them immediately. Hence, write locks on pages must be released at the end of the txn after freeing all the pages.
	toFree := make([]nodeReleaser, 0)
	defer func() {
		for _, node := range toFree {
			tree.pager.FreeNode(txn, node.GetPageId())
			node.Release()
		}
	}()

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		stack = stack[:len(stack)-1]
		if popped.IsLeaf() {
			index, _ := tree.FindKey(txn, popped, key)
			popped.DeleteAt(txn, index)
		}

		if len(stack) == 0 {
			// if no parent left in stack(this is correct only if popped is root) it is done
			popped.Release()
			return true
		}

		if tree.isUnderFlow(popped) {
			indexAtParent := stack[len(stack)-1].Index
			parent := stack[len(stack)-1].Node

			// fetch siblings to merge or distribute with. do not forget to release them.
			var rightSibling, leftSibling, merged nodeReleaser
			if indexAtParent > 0 {
				leftSibling = tree.pager.GetNodeReleaser(txn, parent.GetValueAt(txn, indexAtParent-1).(Pointer), Delete) //leftSibling = parent.Pointers[indexAtParent-1].(*InternalNode)
			}
			if indexAtParent+1 < (parent.KeyLen() + 1) { // +1 is the length of pointers
				rightSibling = tree.pager.GetNodeReleaser(txn, parent.GetValueAt(txn, indexAtParent+1).(Pointer), Delete) //rightSibling = parent.Pointers[indexAtParent+1].(*InternalNode)
			}

			// try redistribute
			// TODO: redistribute based on byte size
			if rightSibling != nil && tree.canRedistribute(popped, rightSibling) {
				tree.redistribute(txn, popped, rightSibling, parent)
				popped.Release()
				rightSibling.Release()

				if leftSibling != nil {
					leftSibling.Release()
				}
				return true
			} else if leftSibling != nil && tree.canRedistribute(popped, leftSibling) {
				tree.redistribute(txn, leftSibling, popped, parent)
				popped.Release()
				leftSibling.Release()

				if rightSibling != nil {
					rightSibling.Release()
				}
				return true
			}

			// if redistribution is not valid merge
			if rightSibling != nil {
				tree.mergeNodes(txn, popped, rightSibling, parent)
				merged = popped

				toFree = append(toFree, rightSibling)

				popped.Release()

				if leftSibling != nil {
					leftSibling.Release()
				}
			} else if leftSibling != nil {
				tree.mergeNodes(txn, leftSibling, popped, parent)
				merged = leftSibling

				leftSibling.Release()

				toFree = append(toFree, popped)
			} else {
				common.Assert(popped.IsLeaf(), "Both siblings are null for an internal node! This should not be possible except for root")
				popped.Release()
				// NOTE: maybe log here while debugging? if it is a leaf node its both left and right nodes can be nil
				return true
			}
			if rootLocked && parent.GetPageId() == tree.getRoot(txn) && parent.KeyLen() == 0 {
				tree.setRoot(txn, merged.GetPageId())
			}
		} else {
			popped.Release()
			break
		}
	}

	return true
}

func (tree *BTree) Get(txn transaction.Transaction, key common.Key) any {
	res, stack := tree.FindAndGetStack(txn, key, Read)
	for _, pair := range stack {
		pair.Node.Release()
	}

	return res
}

func (tree *BTree) FindBetween(start, end common.Key, limit int) []any {
	it := NewTreeIteratorWithKey(transaction.TxnNoop(), start, tree)
	res := make([]any, 0)
	for key, val := it.Next(); val != nil; _, val = it.Next() {
		if end != nil && !key.Less(end) {
			break
		}
		res = append(res, val)
		if limit != 0 && len(res) == limit {
			break
		}
	}

	err := it.Close()
	CheckErr(err)

	return res
}

func (tree *BTree) Height(txn transaction.Transaction) int {
	pager := tree.pager
	var currentNode = tree.GetRoot(txn, Read)
	acc := 0
	for {
		if currentNode.IsLeaf() {
			currentNode.Release()
			return acc + 1
		} else {
			old := currentNode
			currentNode = pager.GetNodeReleaser(txn, currentNode.GetValueAt(txn, 0).(Pointer), Read)
			old.Release()
		}
		acc++
	}
}

func (tree *BTree) Count(txn transaction.Transaction) int {
	tree.rootEntryLock.RLock()
	n := tree.GetRoot(txn, Read)
	tree.rootEntryLock.RUnlock()
	for {
		if n.IsLeaf() {
			break
		}
		old := n
		n = tree.pager.GetNodeReleaser(txn, n.GetValueAt(txn, 0).(Pointer), Read)
		old.Release()
	}

	num := 0
	for {
		num += n.KeyLen()

		r := n.GetRight()
		if r == 0 {
			n.Release()
			break
		}

		old := n
		n = tree.pager.GetNodeReleaser(txn, r, Read)
		old.Release()
	}

	return num
}

func (tree *BTree) Print(txn transaction.Transaction) {
	pager := tree.pager
	queue := make([]Pointer, 0, 2)
	queue = append(queue, tree.getRoot(txn))
	queue = append(queue, 0)
	for i := 0; i < len(queue); i++ {
		if queue[i] == 0 {
			queue = append(queue, 0)
			continue
		}

		node := tree.pager.GetNodeReleaser(txn, queue[i], Read)
		if node.IsLeaf() {
			node.Release()
			break
		}

		pointers := make([]Pointer, 0)
		vals := node.GetValues(txn)
		for _, val := range vals {
			pointers = append(pointers, val.(Pointer))
		}
		queue = append(queue, pointers...)
		node.Release()
	}
	for _, n := range queue {
		if n != 0 {
			currNode := pager.GetNodeReleaser(txn, n, Read)
			currNode.PrintNode(txn)
			currNode.Release()
		} else {
			log.Print("\n ### \n")
		}
	}
}

func (tree *BTree) findAndGetStack(txn transaction.Transaction, node nodeReleaser, key common.Key, stackIn []NodeIndexPair, mode TraverseMode) (value any, stackOut []NodeIndexPair) {
	if node.IsLeaf() {
		i, found := tree.FindKey(txn, node, key)
		stackOut = append(stackIn, NodeIndexPair{node, i})
		if !found {
			return nil, stackOut
		}

		return node.GetValueAt(txn, i), stackOut
	} else {
		i, found := tree.FindKey(txn, node, key)
		if found {
			i++
		}
		stackOut = append(stackIn, NodeIndexPair{node, i})
		pointer := node.GetValueAt(txn, i).(Pointer)
		childNode := tree.pager.GetNodeReleaser(txn, pointer, mode)

		if mode == Read {
			// NOTE: root entry lock is released in FindAndGetStack
			stackOut = stackOut[1:] // TODO: do this better
			node.Release()
		} else if mode == Debug {
			// do nothing return whole stack
		} else {
			// determine if child node is safe
			var safe bool
			if mode == Insert {
				safe = tree.safeForSplit(childNode)
			} else if mode == Delete {
				safe = tree.safeForMerge(childNode)
			}

			// if it is safe release all write locks above
			if safe {
				for _, pair := range stackOut {
					if pair.Index == -1 {
						tree.rootEntryLock.Unlock()
					} else {
						pair.Node.Release()
					}
				}
				stackOut = stackOut[:0]
			}
		}

		res, stackOut := tree.findAndGetStack(txn, childNode, key, stackOut, mode)
		return res, stackOut
	}
}

// FindAndGetStack is used to recursively find the given key, and it also passes a stack object recursively to
// keep the path it followed down to leaf node. value is nil when key does not exist.
func (tree *BTree) FindAndGetStack(txn transaction.Transaction, key common.Key, mode TraverseMode) (value any, stackOut []NodeIndexPair) {
	var stack []NodeIndexPair
	tree.rootEntryLock.Lock()
	root := tree.GetRoot(txn, mode)
	if mode == Insert || mode == Delete {
		// this is a special NodeIndexPair which indicates that root entry lock is acquired and caller should release it.
		stack = append(stack, NodeIndexPair{Index: -1})
	} else {
		// in read mode there is no way root will be split hence we can release entry lock directly
		tree.rootEntryLock.Unlock()
	}
	return tree.findAndGetStack(txn, root, key, stack, mode)
}

func (tree *BTree) mergeInternalNodes(txn transaction.Transaction, p, rightNode, parent node) {
	var i int
	for i = 0; parent.GetValueAt(txn, i).(Pointer) != p.GetPageId(); i++ {
	}

	for ii := 0; ii < rightNode.KeyLen()+1; ii++ {
		var k common.Key
		if ii == 0 {
			k = parent.GetKeyAt(txn, i)
		} else {
			k = rightNode.GetKeyAt(txn, ii-1)
		}
		v := rightNode.GetValueAt(txn, ii)
		p.InsertAt(txn, p.KeyLen(), k, v)
	}
	parent.DeleteAt(txn, i)
}

func (tree *BTree) mergeLeafNodes(txn transaction.Transaction, p, rightNode, parent node) {
	var i int
	for i = 0; parent.GetValueAt(txn, i).(Pointer) != p.GetPageId(); i++ {
	}

	for i := 0; i < rightNode.KeyLen(); i++ {
		p.InsertAt(txn, p.KeyLen(), rightNode.GetKeyAt(txn, i), rightNode.GetValueAt(txn, i))
	}

	parent.DeleteAt(txn, i)
	leftHeader := p.GetHeader()
	leftHeader.Right = rightNode.GetHeader().Right
	p.SetHeader(txn, leftHeader)
}

// mergeNodes merges two nodes into one. Left node holds all the values after merge and rightNode becomes empty. Caller
// should free rightNode.
func (tree *BTree) mergeNodes(txn transaction.Transaction, p, rightNode, parent node) {
	if p.IsLeaf() {
		tree.mergeLeafNodes(txn, p, rightNode, parent)
	} else {
		tree.mergeInternalNodes(txn, p, rightNode, parent)
	}
}

func (tree *BTree) redistributeInternalNodes(txn transaction.Transaction, leftNode, rightNode, parent node) {
	// find left node pointer in the parent
	var i int
	for i = 0; parent.GetValueAt(txn, i).(Pointer) != leftNode.GetPageId(); i++ {
	}

	totalFillFactor := leftNode.FillFactor() + rightNode.FillFactor()
	fillFactorAfterRedistribute := totalFillFactor / 2

	if leftNode.FillFactor() < fillFactorAfterRedistribute {
		// insert new keys to left
		for {
			leftNode.InsertAt(txn, leftNode.KeyLen(), parent.GetKeyAt(txn, i), rightNode.GetValueAt(txn, 0))
			parent.SetKeyAt(txn, i, rightNode.GetKeyAt(txn, 0))
			cutFromInternalNode(txn, rightNode)

			if rightNode.FillFactor() <= fillFactorAfterRedistribute {
				break
			}
		}
	} else {
		for {
			pushToInternalNode(txn, rightNode, leftNode.GetValueAt(txn, leftNode.KeyLen()), parent.GetKeyAt(txn, i))
			parent.SetKeyAt(txn, i, leftNode.GetKeyAt(txn, leftNode.KeyLen()-1))
			leftNode.DeleteAt(txn, leftNode.KeyLen()-1)

			if leftNode.FillFactor() <= fillFactorAfterRedistribute {
				break
			}
		}
	}
}

func (tree *BTree) redistributeLeafNodes(txn transaction.Transaction, leftNode, rightNode, parent node) {
	// find left node pointer in the parent
	var i int
	for i = 0; parent.GetValueAt(txn, i).(Pointer) != leftNode.GetPageId(); i++ {
	}

	totalFillFactor := leftNode.FillFactor() + rightNode.FillFactor()
	totalFillFactorInLeftAfterRedistribute := totalFillFactor / 2
	totalFillFactorInRightAfterRedistribute := totalFillFactor - totalFillFactorInLeftAfterRedistribute

	if leftNode.FillFactor() < totalFillFactorInLeftAfterRedistribute {
		// insert new keys to left
		for {
			leftNode.InsertAt(txn, leftNode.KeyLen(), rightNode.GetKeyAt(txn, 0), rightNode.GetValueAt(txn, 0))
			rightNode.DeleteAt(txn, 0)

			if leftNode.FillFactor() >= totalFillFactorInLeftAfterRedistribute {
				break
			}
		}
	} else {
		for {
			rightNode.InsertAt(txn, 0, leftNode.GetKeyAt(txn, leftNode.KeyLen()-1), leftNode.GetValueAt(txn, leftNode.KeyLen()-1))
			leftNode.DeleteAt(txn, leftNode.KeyLen()-1)

			if rightNode.FillFactor() >= totalFillFactorInRightAfterRedistribute {
				break
			}
		}
	}

	parent.SetKeyAt(txn, i, rightNode.GetKeyAt(txn, 0))
}

func (tree *BTree) redistribute(txn transaction.Transaction, p, rightNode, parent node) {
	if p.IsLeaf() {
		tree.redistributeLeafNodes(txn, p, rightNode, parent)
	} else {
		tree.redistributeInternalNodes(txn, p, rightNode, parent)
	}
}

func (tree *BTree) splitInternalNode(txn transaction.Transaction, p node) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	fillFactor := p.FillFactor()
	minFillFactorAfterSplit := fillFactor / 2

	// create right node and insert into it
	rightNode, err := tree.pager.NewInternalNode(txn, Pointer(0))
	CheckErr(err)

	defer rightNode.Release()

	keys := make([]common.Key, 0)
	values := make([]any, 0)
	for {
		k, v := p.GetKeyAt(txn, p.KeyLen()-1), p.GetValueAt(txn, p.KeyLen())
		keys = append(keys, k)
		values = append(values, v)

		p.DeleteAt(txn, p.KeyLen()-1)

		if p.FillFactor() <= minFillFactorAfterSplit+1 {
			break
		}
	}

	// keyAtLeft is the last key in left node after split and keyAtRight is the key which is pushed up. it is actually
	// not in rightNode poor naming :(
	keyAtLeft = p.GetKeyAt(txn, p.KeyLen()-1)
	keyAtRight = keys[len(keys)-1]

	rightNode.SetValueAt(txn, 0, values[len(values)-1])
	for i := len(values) - 2; i >= 0; i-- {
		rightNode.InsertAt(txn, len(values)-2-i, keys[i], values[i])
	}

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (tree *BTree) splitLeafNode(txn transaction.Transaction, p node) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	fillFactor := p.FillFactor()
	minFillFactorAfterSplit := fillFactor / 2

	rightNode, err := tree.pager.NewLeafNode(txn)
	CheckErr(err)

	defer rightNode.Release()

	for {
		rightNode.InsertAt(txn, 0, p.GetKeyAt(txn, p.KeyLen()-1), p.GetValueAt(txn, p.KeyLen()-1))
		p.DeleteAt(txn, p.KeyLen()-1)

		if rightNode.FillFactor() >= minFillFactorAfterSplit {
			break
		}
	}

	keyAtLeft = p.GetKeyAt(txn, p.KeyLen()-1)
	keyAtRight = rightNode.GetKeyAt(txn, 0)

	leftHeader, rightHeader := p.GetHeader(), rightNode.GetHeader()
	rightHeader.Right = leftHeader.Right
	rightHeader.Left = p.GetPageId()
	leftHeader.Right = rightNode.GetPageId()
	p.SetHeader(txn, leftHeader)
	rightNode.SetHeader(txn, rightHeader)

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (tree *BTree) splitNode(txn transaction.Transaction, p node) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	if p.IsLeaf() {
		return tree.splitLeafNode(txn, p)
	} else {
		return tree.splitInternalNode(txn, p)
	}
}

func (tree *BTree) FindKey(txn transaction.Transaction, p node, key common.Key) (index int, found bool) {
	h := p.GetHeader()
	i := sort.Search(int(h.KeyLen), func(i int) bool {
		return key.Less(p.GetKeyAt(txn, i))
	})

	if i > 0 && !p.GetKeyAt(txn, i-1).Less(key) {
		return i - 1, true
	}
	return i, false
}

func release(stack []NodeIndexPair) {
	for _, pair := range stack {
		pair.Node.Release()
	}
}

func (tree *BTree) isOverFlow(sp node) bool {
	return sp.FillFactor() > tree.overFlowThreshold
}

func (tree *BTree) isUnderFlow(sp node) bool {
	return sp.FillFactor() < tree.underFlowThreshold
}

func (tree *BTree) canMerge(underFlowed, sibling node) bool {
	return underFlowed.FillFactor()+sibling.FillFactor() < tree.overFlowThreshold
}

func (tree *BTree) canRedistribute(underFlowed, sibling node) bool {
	return !tree.canMerge(underFlowed, sibling)
}

func (tree *BTree) safeForMerge(sp node) bool {
	return sp.FillFactor()-1 > tree.underFlowThreshold
}

func (tree *BTree) safeForSplit(sp node) bool {
	return sp.FillFactor()+1 < tree.overFlowThreshold
}

func pushToInternalNode(txn transaction.Transaction, node node, val any, key common.Key) {
	node.InsertAt(txn, 0, key, node.GetValueAt(txn, 0))
	node.SetValueAt(txn, 0, val)
}

func cutFromInternalNode(txn transaction.Transaction, node node) {
	node.SetValueAt(txn, 0, node.GetValueAt(txn, 1))
	node.DeleteAt(txn, 0)
}
