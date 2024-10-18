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

func (tree *BTree) Insert(txn transaction.Transaction, key common.Key, value any) error {
	i, stack, err := tree.FindAndGetStack(txn, key, Insert)
	if err != nil {
		return err
	}

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
		i, _, err := tree.findKey(txn, popped, key)
		if err != nil {
			return err
		}

		if err := popped.InsertAt(txn, i, rightKey, rightNod); err != nil {
			return err
		}

		if tree.isOverFlow(popped) {
			var err error
			rightNod, _, rightKey, err = tree.splitNode(txn, popped)
			if err != nil {
				return err
			}

			if rootLocked && popped.GetPageId() == tree.getRoot(txn) {
				leftNode := popped

				newRoot, err := tree.pager.NewInternalNode(txn, leftNode.GetPageId())
				if err != nil {
					return err
				}

				if err := newRoot.InsertAt(txn, 0, rightKey, rightNod.(Pointer)); err != nil {
					return err
				}

				tree.setRoot(txn, newRoot.GetPageId())

				newRoot.Release()
			}

			popped.Release()
		} else {
			popped.Release()
			break
		}
	}

	return nil
}

func (tree *BTree) Set(txn transaction.Transaction, key common.Key, value any) (isInserted bool, err error) {
	i, stack, err := tree.FindAndGetStack(txn, key, Insert)
	if err != nil {
		return false, err
	}

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
		if err := leafNode.SetValueAt(txn, topOfStack.Index, value); err != nil {
			return false, err
		}

		stack = stack[:len(stack)-1]
		topOfStack.Node.Release()
		return false, nil
	}

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		stack = stack[:len(stack)-1]

		i, _, err := tree.findKey(txn, popped, key)
		if err != nil {
			return false, err
		}

		if err := popped.InsertAt(txn, i, rightKey, rightNod); err != nil {
			return false, err
		}

		if tree.isOverFlow(popped) {
			var err error
			rightNod, _, rightKey, err = tree.splitNode(txn, popped)
			if err != nil {
				return false, err
			}

			popped.Release()
			if rootLocked && popped.GetPageId() == tree.getRoot(txn) {
				leftNode := popped

				newRoot, err := tree.pager.NewInternalNode(txn, leftNode.GetPageId())
				if err != nil {
					return false, err
				}

				if err := newRoot.InsertAt(txn, 0, rightKey, rightNod.(Pointer)); err != nil {
					return false, err
				}

				tree.setRoot(txn, newRoot.GetPageId())
				newRoot.Release()
			}
		} else {
			popped.Release()
			break
		}
	}

	return true, nil
}

func (tree *BTree) Delete(txn transaction.Transaction, key common.Key) (bool, error) {
	i, stack, err := tree.FindAndGetStack(txn, key, Delete)
	if err != nil {
		return false, err
	}

	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1 {
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	defer func() { release(stack) }()
	if i == nil {
		return false, nil
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
			index, _, err := tree.findKey(txn, popped, key)
			if err != nil {
				return false, err
			}

			if err := popped.DeleteAt(txn, index); err != nil {
				return false, err
			}
		}

		if len(stack) == 0 {
			// if no parent left in stack(this is correct only if popped is root) it is done
			popped.Release()
			return true, nil
		}

		if tree.isUnderFlow(popped) {
			indexAtParent := stack[len(stack)-1].Index
			parent := stack[len(stack)-1].Node

			// fetch siblings to merge or distribute with. do not forget to release them.
			var rightSibling, leftSibling, merged nodeReleaser
			if indexAtParent > 0 {
				v, err := parent.GetValueAt(txn, indexAtParent-1)
				if err != nil {
					return false, err
				}

				leftSibling = tree.pager.GetNodeReleaser(txn, v.(Pointer), Delete) //leftSibling = parent.Pointers[indexAtParent-1].(*InternalNode)
			}
			if indexAtParent+1 < (parent.KeyLen() + 1) { // +1 is the length of pointers
				v, err := parent.GetValueAt(txn, indexAtParent+1)
				if err != nil {
					return false, err
				}

				rightSibling = tree.pager.GetNodeReleaser(txn, v.(Pointer), Delete) //rightSibling = parent.Pointers[indexAtParent+1].(*InternalNode)
			}

			// try redistribute
			// TODO: redistribute based on byte size
			if rightSibling != nil && tree.canRedistribute(popped, rightSibling) {
				if err := tree.redistribute(txn, popped, rightSibling, parent); err != nil {
					return false, err
				}

				popped.Release()
				rightSibling.Release()

				if leftSibling != nil {
					leftSibling.Release()
				}

				return true, nil
			} else if leftSibling != nil && tree.canRedistribute(popped, leftSibling) {
				if err := tree.redistribute(txn, leftSibling, popped, parent); err != nil {
					return false, err
				}
				popped.Release()
				leftSibling.Release()

				if rightSibling != nil {
					rightSibling.Release()
				}

				return true, nil
			}

			// if redistribution is not valid merge
			if rightSibling != nil {
				if err := tree.mergeNodes(txn, popped, rightSibling, parent); err != nil {
					return false, err
				}

				merged = popped

				toFree = append(toFree, rightSibling)

				popped.Release()

				if leftSibling != nil {
					leftSibling.Release()
				}
			} else if leftSibling != nil {
				if err := tree.mergeNodes(txn, leftSibling, popped, parent); err != nil {
					return false, err
				}

				merged = leftSibling

				leftSibling.Release()

				toFree = append(toFree, popped)
			} else {
				common.Assert(popped.IsLeaf(), "Both siblings are null for an internal node! This should not be possible except for root")
				popped.Release()
				// NOTE: maybe log here while debugging? if it is a leaf node its both left and right nodes can be nil
				return true, nil
			}
			if rootLocked && parent.GetPageId() == tree.getRoot(txn) && parent.KeyLen() == 0 {
				tree.setRoot(txn, merged.GetPageId())
			}
		} else {
			popped.Release()
			break
		}
	}

	return true, nil
}

func (tree *BTree) Get(txn transaction.Transaction, key common.Key) (any, error) {
	res, stack, err := tree.FindAndGetStack(txn, key, Read)
	if err != nil {
		return nil, err
	}

	for _, pair := range stack {
		pair.Node.Release()
	}

	return res, nil
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

func (tree *BTree) Height(txn transaction.Transaction) (int, error) {
	pager := tree.pager
	var currentNode = tree.GetRoot(txn, Read)
	acc := 0
	for {
		if currentNode.IsLeaf() {
			currentNode.Release()
			return acc + 1, nil
		} else {
			old := currentNode

			v, err := currentNode.GetValueAt(txn, 0)
			if err != nil {
				return 0, err
			}

			currentNode = pager.GetNodeReleaser(txn, v.(Pointer), Read)
			old.Release()
		}
		acc++
	}
}

func (tree *BTree) Count(txn transaction.Transaction) (int, error) {
	tree.rootEntryLock.RLock()
	n := tree.GetRoot(txn, Read)
	tree.rootEntryLock.RUnlock()
	for {
		if n.IsLeaf() {
			break
		}
		old := n

		v, err := n.GetValueAt(txn, 0)
		if err != nil {
			return 0, err
		}

		n = tree.pager.GetNodeReleaser(txn, v.(Pointer), Read)
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

	return num, nil
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
		vals, err := node.GetValues(txn)
		common.PanicIfErr(err)

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

func (tree *BTree) findAndGetStack(txn transaction.Transaction, node nodeReleaser, key common.Key, stackIn []NodeIndexPair, mode TraverseMode) (value any, stackOut []NodeIndexPair, err error) {
	if node.IsLeaf() {
		i, found, err := tree.findKey(txn, node, key)
		if err != nil {
			return nil, nil, err
		}

		stackOut = append(stackIn, NodeIndexPair{node, i})
		if !found {
			return nil, stackOut, nil
		}

		v, err := node.GetValueAt(txn, i)
		return v, stackOut, err
	} else {
		i, found, err := tree.findKey(txn, node, key)
		if err != nil {
			return nil, nil, err
		}

		if found {
			i++
		}
		stackOut = append(stackIn, NodeIndexPair{node, i})

		v, err := node.GetValueAt(txn, i)
		if err != nil {
			return nil, nil, err
		}

		pointer := v.(Pointer)
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

		res, stackOut, err := tree.findAndGetStack(txn, childNode, key, stackOut, mode)
		if err != nil {
			return nil, nil, err
		}

		return res, stackOut, nil
	}
}

// FindAndGetStack is used to recursively find the given key, and it also passes a stack object recursively to
// keep the path it followed down to leaf node. value is nil when key does not exist.
func (tree *BTree) FindAndGetStack(txn transaction.Transaction, key common.Key, mode TraverseMode) (value any, stackOut []NodeIndexPair, err error) {
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

func (tree *BTree) mergeInternalNodes(txn transaction.Transaction, p, rightNode, parent node) error {
	var i int
	for i = 0; ; i++ {
		v, err := parent.GetValueAt(txn, i)
		if err != nil {
			return err
		}

		if v.(Pointer) == p.GetPageId() {
			break
		}
	}

	for ii := 0; ii < rightNode.KeyLen()+1; ii++ {
		var k common.Key
		var err error
		if ii == 0 {
			k, err = parent.GetKeyAt(txn, i)
			if err != nil {
				return err
			}

		} else {
			k, err = rightNode.GetKeyAt(txn, ii-1)
			if err != nil {
				return err
			}
		}
		v, err := rightNode.GetValueAt(txn, ii)
		if err != nil {
			return err
		}

		if err := p.InsertAt(txn, p.KeyLen(), k, v); err != nil {
			return err
		}
	}
	if err := parent.DeleteAt(txn, i); err != nil {
		return err
	}

	return nil
}

func (tree *BTree) mergeLeafNodes(txn transaction.Transaction, p, rightNode, parent node) error {
	var i int
	for i = 0; ; i++ {
		v, err := parent.GetValueAt(txn, i)
		if err != nil {
			return err
		}

		if v.(Pointer) == p.GetPageId() {
			break
		}
	}

	for i := 0; i < rightNode.KeyLen(); i++ {
		k, err := rightNode.GetKeyAt(txn, i)
		if err != nil {
			return err
		}

		v, err := rightNode.GetValueAt(txn, i)
		if err != nil {
			return err
		}

		if err := p.InsertAt(txn, p.KeyLen(), k, v); err != nil {
			return err
		}
	}

	if err := parent.DeleteAt(txn, i); err != nil {
		return err
	}

	leftHeader := p.GetHeader()
	leftHeader.Right = rightNode.GetHeader().Right
	p.SetHeader(txn, leftHeader)

	return nil
}

// mergeNodes merges two nodes into one. Left node holds all the values after merge and rightNode becomes empty. Caller
// should free rightNode.
func (tree *BTree) mergeNodes(txn transaction.Transaction, p, rightNode, parent node) error {
	if p.IsLeaf() {
		return tree.mergeLeafNodes(txn, p, rightNode, parent)
	} else {
		return tree.mergeInternalNodes(txn, p, rightNode, parent)
	}
}

func (tree *BTree) redistributeInternalNodes(txn transaction.Transaction, leftNode, rightNode, parent node) error {
	// find left node pointer in the parent
	var i int
	for i = 0; ; i++ {
		v, err := parent.GetValueAt(txn, i)
		if err != nil {
			return err
		}

		if v.(Pointer) == leftNode.GetPageId() {
			break
		}
	}

	totalFillFactor := leftNode.FillFactor() + rightNode.FillFactor()
	fillFactorAfterRedistribute := totalFillFactor / 2

	if leftNode.FillFactor() < fillFactorAfterRedistribute {
		// insert new keys to left
		for {
			k, err := parent.GetKeyAt(txn, i)
			if err != nil {
				return err
			}

			v, err := rightNode.GetValueAt(txn, 0)
			if err != nil {
				return err
			}

			if err := leftNode.InsertAt(txn, leftNode.KeyLen(), k, v); err != nil {
				return err
			}

			k0, err := rightNode.GetKeyAt(txn, 0)
			if err != nil {
				return err
			}

			if err := parent.SetKeyAt(txn, i, k0); err != nil {
				return err
			}

			if err := cutFromInternalNode(txn, rightNode); err != nil {
				return err
			}

			if rightNode.FillFactor() <= fillFactorAfterRedistribute {
				break
			}
		}
	} else {
		for {
			v, err := leftNode.GetValueAt(txn, leftNode.KeyLen())
			if err != nil {
				return err
			}

			k, err := parent.GetKeyAt(txn, i)
			if err != nil {
				return err
			}

			if err := pushToInternalNode(txn, rightNode, v, k); err != nil {
				return err
			}

			k, err = leftNode.GetKeyAt(txn, leftNode.KeyLen()-1)
			if err != nil {
				return err
			}

			if err := parent.SetKeyAt(txn, i, k); err != nil {
				return err
			}

			if err := leftNode.DeleteAt(txn, leftNode.KeyLen()-1); err != nil {
				return err
			}

			if leftNode.FillFactor() <= fillFactorAfterRedistribute {
				break
			}
		}
	}

	return nil
}

func (tree *BTree) redistributeLeafNodes(txn transaction.Transaction, leftNode, rightNode, parent node) error {
	// find left node pointer in the parent
	var i int
	for i = 0; ; i++ {
		v, err := parent.GetValueAt(txn, i)
		if err != nil {
			return err
		}

		if v.(Pointer) == leftNode.GetPageId() {
			break
		}
	}

	totalFillFactor := leftNode.FillFactor() + rightNode.FillFactor()
	totalFillFactorInLeftAfterRedistribute := totalFillFactor / 2
	totalFillFactorInRightAfterRedistribute := totalFillFactor - totalFillFactorInLeftAfterRedistribute

	if leftNode.FillFactor() < totalFillFactorInLeftAfterRedistribute {
		// insert new keys to left
		for {
			k, err := rightNode.GetKeyAt(txn, 0)
			if err != nil {
				return err
			}

			v, err := rightNode.GetValueAt(txn, 0)
			if err != nil {
				return err
			}

			if err := leftNode.InsertAt(txn, leftNode.KeyLen(), k, v); err != nil {
				return err
			}

			if err := rightNode.DeleteAt(txn, 0); err != nil {
				return err
			}

			if leftNode.FillFactor() >= totalFillFactorInLeftAfterRedistribute {
				break
			}
		}
	} else {
		for {
			k, err := leftNode.GetKeyAt(txn, leftNode.KeyLen()-1)
			if err != nil {
				return err
			}

			v, err := leftNode.GetValueAt(txn, leftNode.KeyLen()-1)
			if err != nil {
				return err
			}

			if err := rightNode.InsertAt(txn, 0, k, v); err != nil {
				return err
			}

			if err := leftNode.DeleteAt(txn, leftNode.KeyLen()-1); err != nil {
				return err
			}

			if rightNode.FillFactor() >= totalFillFactorInRightAfterRedistribute {
				break
			}
		}
	}

	k0, err := rightNode.GetKeyAt(txn, 0)
	if err != nil {
		return err
	}

	if err := parent.SetKeyAt(txn, i, k0); err != nil {
		return err
	}

	return nil
}

func (tree *BTree) redistribute(txn transaction.Transaction, p, rightNode, parent node) error {
	if p.IsLeaf() {
		return tree.redistributeLeafNodes(txn, p, rightNode, parent)
	} else {
		return tree.redistributeInternalNodes(txn, p, rightNode, parent)
	}
}

func (tree *BTree) splitInternalNode(txn transaction.Transaction, p node) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key, err error) {
	fillFactor := p.FillFactor()
	minFillFactorAfterSplit := fillFactor / 2

	// create right node and insert into it
	rightNode, err := tree.pager.NewInternalNode(txn, Pointer(0))
	if err != nil {
		return 0, nil, nil, err
	}

	defer rightNode.Release()

	keys := make([]common.Key, 0)
	values := make([]any, 0)
	for {
		k, err := p.GetKeyAt(txn, p.KeyLen()-1)
		if err != nil {
			return 0, nil, nil, err
		}

		v, err := p.GetValueAt(txn, p.KeyLen())
		if err != nil {
			return 0, nil, nil, err
		}

		keys = append(keys, k)
		values = append(values, v)

		if err := p.DeleteAt(txn, p.KeyLen()-1); err != nil {
			return 0, nil, nil, err
		}

		if p.FillFactor() <= minFillFactorAfterSplit+1 {
			break
		}
	}

	// keyAtLeft is the last key in left node after split and keyAtRight is the key which is pushed up. it is actually
	// not in rightNode poor naming :(
	keyAtLeft, err = p.GetKeyAt(txn, p.KeyLen()-1)
	if err != nil {
		return 0, nil, nil, err
	}

	keyAtRight = keys[len(keys)-1]

	if err := rightNode.SetValueAt(txn, 0, values[len(values)-1]); err != nil {
		return 0, nil, nil, err
	}

	for i := len(values) - 2; i >= 0; i-- {
		if err := rightNode.InsertAt(txn, len(values)-2-i, keys[i], values[i]); err != nil {
			return 0, nil, nil, err
		}
	}

	return rightNode.GetPageId(), keyAtLeft, keyAtRight, nil
}

func (tree *BTree) splitLeafNode(txn transaction.Transaction, p node) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key, err error) {
	fillFactor := p.FillFactor()
	minFillFactorAfterSplit := fillFactor / 2

	rightNode, err := tree.pager.NewLeafNode(txn)
	if err != nil {
		return 0, nil, nil, err
	}

	defer rightNode.Release()

	for {
		k, err := p.GetKeyAt(txn, p.KeyLen()-1)
		if err != nil {
			return 0, nil, nil, err
		}

		v, err := p.GetValueAt(txn, p.KeyLen()-1)
		if err != nil {
			return 0, nil, nil, err
		}

		if err := rightNode.InsertAt(txn, 0, k, v); err != nil {
			return 0, nil, nil, err
		}

		if err := p.DeleteAt(txn, p.KeyLen()-1); err != nil {
			return 0, nil, nil, err
		}

		if rightNode.FillFactor() >= minFillFactorAfterSplit {
			break
		}
	}

	keyAtLeft, err = p.GetKeyAt(txn, p.KeyLen()-1)
	if err != nil {
		return 0, nil, nil, err
	}

	keyAtRight, err = rightNode.GetKeyAt(txn, 0)
	if err != nil {
		return 0, nil, nil, err
	}

	leftHeader, rightHeader := p.GetHeader(), rightNode.GetHeader()
	rightHeader.Right = leftHeader.Right
	rightHeader.Left = p.GetPageId()
	leftHeader.Right = rightNode.GetPageId()
	p.SetHeader(txn, leftHeader)
	rightNode.SetHeader(txn, rightHeader)

	return rightNode.GetPageId(), keyAtLeft, keyAtRight, nil
}

func (tree *BTree) splitNode(txn transaction.Transaction, p node) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key, err error) {
	if p.IsLeaf() {
		return tree.splitLeafNode(txn, p)
	} else {
		return tree.splitInternalNode(txn, p)
	}
}

func (tree *BTree) findKey(txn transaction.Transaction, p node, key common.Key) (index int, found bool, err error) {
	h := p.GetHeader()
	var sortErr error

	// TODO: better error handling
	i := sort.Search(int(h.KeyLen), func(i int) bool {
		if sortErr != nil {
			return false
		}

		k, err := p.GetKeyAt(txn, i)
		if err != nil {
			sortErr = err
			return false
		}

		return key.Less(k)
	})

	if sortErr != nil {
		return 0, false, sortErr
	}

	if i > 0 {
		k, err := p.GetKeyAt(txn, i-1)
		if err != nil {
			return 0, false, err
		}

		if !k.Less(key) {
			return i - 1, true, nil
		}
	}

	return i, false, nil
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

func pushToInternalNode(txn transaction.Transaction, node node, val any, key common.Key) error {
	v, err := node.GetValueAt(txn, 0)
	if err != nil {
		return err
	}

	if err := node.InsertAt(txn, 0, key, v); err != nil {
		return err
	}

	if err := node.SetValueAt(txn, 0, val); err != nil {
		return err
	}

	return nil
}

func cutFromInternalNode(txn transaction.Transaction, node node) error {
	v, err := node.GetValueAt(txn, 1)
	if err != nil {
		return err
	}

	if err := node.SetValueAt(txn, 0, v); err != nil {
		return err
	}

	if err := node.DeleteAt(txn, 0); err != nil {
		return err
	}

	return nil
}

func release(stack []NodeIndexPair) {
	for _, pair := range stack {
		pair.Node.Release()
	}
}
