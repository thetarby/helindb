package btree

import (
	"encoding/binary"
	"fmt"
	"helin/common"
	"helin/transaction"
	"math/rand"
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
	meta := metaPage{pager.CreatePage(txn)}
	l := pager.NewLeafNode(txn)
	root := pager.NewInternalNode(txn, l.GetPageId())
	meta.setRoot(txn, root.GetPageId())
	meta.setDegree(txn, degree)

	defer root.Release()
	defer l.Release()
	defer meta.Release()

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

func ConstructBtreeByMeta(metaPID Pointer, pager *Pager2) *BTree {
	meta := metaPage{pager.GetPage(metaPID, true)}
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

func (tree *BTree) GetRoot(mode TraverseMode) NodeReleaser {
	return tree.pager.GetNodeReleaser(tree.getRoot(), mode)
}

func (tree *BTree) meta(readOnly bool) *metaPage {
	meta := tree.pager.GetPage(tree.metaPID, readOnly)
	return &metaPage{meta}
}

func (tree *BTree) getRoot() Pointer {
	meta := tree.meta(true)
	defer meta.Release()

	return meta.getRoot()
}

func (tree *BTree) setRoot(txn transaction.Transaction, p Pointer) {
	meta := tree.meta(false)
	defer meta.Release()
	meta.setRoot(txn, p)
}

func (tree *BTree) GetPager() *Pager2 {
	return tree.pager
}

func (tree *BTree) Insert(txn transaction.Transaction, key common.Key, value any) {
	i, stack := tree.FindAndGetStack(key, Insert)
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
		i, _ := tree.FindKey(popped, key)
		popped.InsertAt(txn, i, rightKey, rightNod)

		if tree.isOverFlow(popped) {
			rightNod, _, rightKey = tree.splitNode(txn, popped, popped.KeyLen()/2)
			if rootLocked && popped.GetPageId() == tree.getRoot() {
				leftNode := popped

				newRoot := tree.pager.NewInternalNode(txn, leftNode.GetPageId())
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
	i, stack := tree.FindAndGetStack(key, Insert)
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
		i, _ := tree.FindKey(popped, key)
		popped.InsertAt(txn, i, rightKey, rightNod)

		if tree.isOverFlow(popped) {
			rightNod, _, rightKey = tree.splitNode(txn, popped, popped.KeyLen()/2)
			popped.Release()
			if rootLocked && popped.GetPageId() == tree.getRoot() {
				leftNode := popped

				newRoot := tree.pager.NewInternalNode(txn, leftNode.GetPageId())
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
	i, stack := tree.FindAndGetStack(key, Delete)
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
	toFree := make([]NodeReleaser, 0)
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
			index, _ := tree.FindKey(popped, key)
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
			var rightSibling, leftSibling, merged NodeReleaser
			if indexAtParent > 0 {
				leftSibling = tree.pager.GetNodeReleaser(parent.GetValueAt(indexAtParent-1).(Pointer), Delete) //leftSibling = parent.Pointers[indexAtParent-1].(*InternalNode)
			}
			if indexAtParent+1 < (parent.KeyLen() + 1) { // +1 is the length of pointers
				rightSibling = tree.pager.GetNodeReleaser(parent.GetValueAt(indexAtParent+1).(Pointer), Delete) //rightSibling = parent.Pointers[indexAtParent+1].(*InternalNode)
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
			if rootLocked && parent.GetPageId() == tree.getRoot() && parent.KeyLen() == 0 {
				tree.setRoot(txn, merged.GetPageId())
			}
		} else {
			popped.Release()
			break
		}
	}

	return true
}

func (tree *BTree) Get(key common.Key) any {
	res, stack := tree.FindAndGetStack(key, Read)
	for _, pair := range stack {
		pair.Node.Release()
	}

	return res
}

func (tree *BTree) FindSince(key common.Key) []any {
	_, stack := tree.FindAndGetStack(key, Read)

	node := stack[len(stack)-1].Node
	vals := node.GetValues()
	res := vals[stack[len(stack)-1].Index:]
	for {
		p := node.GetRight()
		if p == 0 {
			node.Release()
			break
		}
		old := node
		node = tree.pager.GetNodeReleaser(p, Read)
		old.Release()
		vals := node.GetValues()
		res = append(res, vals...)
	}

	return res
}

func (tree *BTree) FindBetween(start, end common.Key, limit int) []any {
	it := NewTreeIteratorWithKey(transaction.TxnNoop(), start, tree)
	res := make([]any, 0)
	for key, val := it.Next(); val != nil; _, val = it.Next() {
		if !key.Less(end) {
			break
		}
		res = append(res, val)
		if len(res) == limit {
			break
		}
	}

	err := it.Close()
	CheckErr(err)

	return res
}

func (tree *BTree) Height() int {
	pager := tree.pager
	var currentNode = tree.GetRoot(Read)
	acc := 0
	for {
		if currentNode.IsLeaf() {
			currentNode.RUnLatch()
			return acc + 1
		} else {
			old := currentNode
			currentNode = pager.GetNodeReleaser(currentNode.GetValueAt(0).(Pointer), Read)
			old.RUnLatch()
		}
		acc++
	}
}

func (tree *BTree) Count() int {
	tree.rootEntryLock.RLock()
	n := tree.GetRoot(Read)
	tree.rootEntryLock.RUnlock()
	for {
		if n.IsLeaf() {
			break
		}
		old := n
		n = tree.pager.GetNodeReleaser(n.GetValueAt(0).(Pointer), Read)
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
		n = tree.pager.GetNodeReleaser(r, Read)
		old.Release()
	}

	return num
}

func (tree *BTree) Print() {
	pager := tree.pager
	queue := make([]Pointer, 0, 2)
	queue = append(queue, tree.getRoot())
	queue = append(queue, 0)
	for i := 0; i < len(queue); i++ {
		if queue[i] == 0 {
			queue = append(queue, 0)
			continue
		}

		node := tree.pager.GetNodeReleaser(queue[i], Read)
		if node.IsLeaf() {
			node.Release()
			break
		}

		pointers := make([]Pointer, 0)
		vals := node.GetValues()
		for _, val := range vals {
			pointers = append(pointers, val.(Pointer))
		}
		queue = append(queue, pointers...)
		node.Release()
	}
	for _, n := range queue {
		if n != 0 {
			currNode := pager.GetNodeReleaser(n, Read)
			currNode.PrintNode()
			currNode.Release()
		} else {
			fmt.Print("\n ### \n")
		}
	}
}

func (tree *BTree) findAndGetStack(node NodeReleaser, key common.Key, stackIn []NodeIndexPair, mode TraverseMode) (value any, stackOut []NodeIndexPair) {
	if node.IsLeaf() {
		i, found := tree.FindKey(node, key)
		stackOut = append(stackIn, NodeIndexPair{node, i})
		if !found {
			return nil, stackOut
		}
		return node.GetValueAt(i), stackOut
	} else {
		i, found := tree.FindKey(node, key)
		if found {
			i++
		}
		stackOut = append(stackIn, NodeIndexPair{node, i})
		pointer := node.GetValueAt(i).(Pointer)
		childNode := tree.pager.GetNodeReleaser(pointer, mode)

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

		res, stackOut := tree.findAndGetStack(childNode, key, stackOut, mode)
		return res, stackOut
	}
}

// FindAndGetStack is used to recursively find the given key, and it also passes a stack object recursively to
// keep the path it followed down to leaf node. value is nil when key does not exist.
func (tree *BTree) FindAndGetStack(key common.Key, mode TraverseMode) (value any, stackOut []NodeIndexPair) {
	var stack []NodeIndexPair
	tree.rootEntryLock.Lock()
	root := tree.GetRoot(mode)
	if mode == Insert || mode == Delete {
		// this is a special NodeIndexPair which indicates that root entry lock is acquired and caller should release it.
		stack = append(stack, NodeIndexPair{Index: -1})
	} else {
		// in read mode there is no way root will be split hence we can release entry lock directly
		tree.rootEntryLock.Unlock()
	}
	return tree.findAndGetStack(root, key, stack, mode)
}

func (tree *BTree) mergeInternalNodes(txn transaction.Transaction, p, rightNode, parent node) {
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	}

	for ii := 0; ii < rightNode.KeyLen()+1; ii++ {
		var k common.Key
		if ii == 0 {
			k = parent.GetKeyAt(i)
		} else {
			k = rightNode.GetKeyAt(ii - 1)
		}
		v := rightNode.GetValueAt(ii)
		p.InsertAt(txn, p.KeyLen(), k, v)
	}
	parent.DeleteAt(txn, i)
}

func (tree *BTree) mergeLeafNodes(txn transaction.Transaction, p, rightNode, parent node) {
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	}

	for i := 0; i < rightNode.KeyLen(); i++ {
		p.InsertAt(txn, p.KeyLen(), rightNode.GetKeyAt(i), rightNode.GetValueAt(i))
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
	for i = 0; parent.GetValueAt(i).(Pointer) != leftNode.GetPageId(); i++ {
	}

	totalFillFactor := leftNode.FillFactor() + rightNode.FillFactor()
	fillFactorAfterRedistribute := totalFillFactor / 2

	if leftNode.FillFactor() < fillFactorAfterRedistribute {
		// insert new keys to left
		for {
			leftNode.InsertAt(txn, leftNode.KeyLen(), parent.GetKeyAt(i), rightNode.GetValueAt(0))
			parent.SetKeyAt(txn, i, rightNode.GetKeyAt(0))
			cutFromInternalNode(txn, rightNode)

			if rightNode.FillFactor() <= fillFactorAfterRedistribute {
				break
			}
		}
	} else {
		for {
			pushToInternalNode(txn, rightNode, leftNode.GetValueAt(leftNode.KeyLen()), parent.GetKeyAt(i))
			parent.SetKeyAt(txn, i, leftNode.GetKeyAt(leftNode.KeyLen()-1))
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
	for i = 0; parent.GetValueAt(i).(Pointer) != leftNode.GetPageId(); i++ {
	}

	totalFillFactor := leftNode.FillFactor() + rightNode.FillFactor()
	totalFillFactorInLeftAfterRedistribute := totalFillFactor / 2
	totalFillFactorInRightAfterRedistribute := totalFillFactor - totalFillFactorInLeftAfterRedistribute

	if leftNode.FillFactor() < totalFillFactorInLeftAfterRedistribute {
		// insert new keys to left
		for {
			leftNode.InsertAt(txn, leftNode.KeyLen(), rightNode.GetKeyAt(0), rightNode.GetValueAt(0))
			rightNode.DeleteAt(txn, 0)

			if leftNode.FillFactor() >= totalFillFactorInLeftAfterRedistribute {
				break
			}
		}
	} else {
		for {
			rightNode.InsertAt(txn, 0, leftNode.GetKeyAt(leftNode.KeyLen()-1), leftNode.GetValueAt(leftNode.KeyLen()-1))
			leftNode.DeleteAt(txn, leftNode.KeyLen()-1)

			if rightNode.FillFactor() >= totalFillFactorInRightAfterRedistribute {
				break
			}
		}
	}

	parent.SetKeyAt(txn, i, rightNode.GetKeyAt(0))
}

func (tree *BTree) redistribute(txn transaction.Transaction, p, rightNode, parent node) {
	if p.IsLeaf() {
		tree.redistributeLeafNodes(txn, p, rightNode, parent)
	} else {
		tree.redistributeInternalNodes(txn, p, rightNode, parent)
	}
}

func (tree *BTree) splitInternalNode(txn transaction.Transaction, p node, idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	if rand.Intn(40) == 1 {
		panic(fmt.Sprintf("whaaaat %v", txn.GetID()))
	}
	fillFactor := p.FillFactor()
	minFillFactorAfterSplit := fillFactor / 2

	// create right node and insert into it
	// corresponding pointer is in the next index that is why +1
	rightNode := tree.pager.NewInternalNode(txn, p.GetValueAt(idx+1).(Pointer))
	defer rightNode.Release()

	keys := make([]common.Key, 0)
	values := make([]any, 0)
	for {
		k, v := p.GetKeyAt(p.KeyLen()-1), p.GetValueAt(p.KeyLen())
		keys = append(keys, k)
		values = append(values, v)

		p.DeleteAt(txn, p.KeyLen()-1)

		if p.FillFactor() <= minFillFactorAfterSplit+1 {
			break
		}
	}

	// keyAtLeft is the last key in left node after split and keyAtRight is the key which is pushed up. it is actually
	// not in rightNode poor naming :(
	keyAtLeft = p.GetKeyAt(p.KeyLen() - 1)
	keyAtRight = keys[len(keys)-1]

	rightNode.SetValueAt(txn, 0, values[len(values)-1])
	for i := len(values) - 2; i >= 0; i-- {
		rightNode.InsertAt(txn, len(values)-2-i, keys[i], values[i])
	}

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (tree *BTree) splitLeafNode(txn transaction.Transaction, p node, idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	fillFactor := p.FillFactor()
	minFillFactorAfterSplit := fillFactor / 2

	rightNode := tree.pager.NewLeafNode(txn)
	defer rightNode.Release()

	for {
		rightNode.InsertAt(txn, 0, p.GetKeyAt(p.KeyLen()-1), p.GetValueAt(p.KeyLen()-1))
		p.DeleteAt(txn, p.KeyLen()-1)

		if rightNode.FillFactor() >= minFillFactorAfterSplit {
			break
		}
	}

	keyAtLeft = p.GetKeyAt(p.KeyLen() - 1)
	keyAtRight = rightNode.GetKeyAt(0)

	leftHeader, rightHeader := p.GetHeader(), rightNode.GetHeader()
	rightHeader.Right = leftHeader.Right
	rightHeader.Left = p.GetPageId()
	leftHeader.Right = rightNode.GetPageId()
	p.SetHeader(txn, leftHeader)
	rightNode.SetHeader(txn, rightHeader)

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (tree *BTree) splitNode(txn transaction.Transaction, p node, idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	if p.IsLeaf() {
		return tree.splitLeafNode(txn, p, idx)
	} else {
		return tree.splitInternalNode(txn, p, idx)
	}
}

func (tree *BTree) FindKey(p node, key common.Key) (index int, found bool) {
	h := p.GetHeader()
	i := sort.Search(int(h.KeyLen), func(i int) bool {
		return key.Less(p.GetKeyAt(i))
	})

	if i > 0 && !p.GetKeyAt(i-1).Less(key) {
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
	node.InsertAt(txn, 0, key, node.GetValueAt(0))
	node.SetValueAt(txn, 0, val)
}

func cutFromInternalNode(txn transaction.Transaction, node node) {
	node.SetValueAt(txn, 0, node.GetValueAt(1))
	node.DeleteAt(txn, 0)
}
