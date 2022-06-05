package btree

// TODO:
// 1. don't use Keys.find instead define one on Node and use that method.
// 2. define util methods to change state of node such as shift_keys_by_n, shift_pointers_by_n, truncate_at_n
//    and use them in methods like InsertAt and SplitNode
// 3. Put a constraint on Key to make it fix sized maybe? It will solve many problems when we try to persist nodes on a disk
// 4. Use interface methods in delete.go as well.

import (
	"fmt"
	"helin/common"
	"sync"
)

type BTree struct {
	degree        int
	length        int
	Root          Pointer
	pager         Pager
	rootEntryLock *sync.RWMutex
}

func NewBtreeWithPager(degree int, pager Pager) *BTree {
	l := pager.NewLeafNode()
	root := pager.NewInternalNode(l.GetPageId())
	defer root.WUnlatch()
	defer l.WUnlatch()
	defer pager.Unpin(root, true)
	defer pager.Unpin(l, true)

	return &BTree{
		degree:         degree,
		length:         0,
		Root:           root.GetPageId(),
		pager:          pager,
		rootEntryLock: &sync.RWMutex{},
	}
}

func ConstructBtreeFromRootPointer(rootPage Pointer, degree int, pager Pager) *BTree {
	// print(1)
	return &BTree{
		degree:        degree,
		length:        0,
		Root:          rootPage,
		pager:         pager,
		rootEntryLock: &sync.RWMutex{},
	}
}

func (tree *BTree) GetRoot(mode TraverseMode) Node {
	return tree.pager.GetNode(tree.Root, mode)
}

func (tree *BTree) GetPager() Pager {
	return tree.pager
}

func (tree *BTree) Insert(key common.Key, value interface{}) {
	i, stack := tree.FindAndGetStack(key, Insert)
	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1{
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	if i != nil {
		panic(fmt.Sprintf("key already exists:  %v", key))
	}
	defer func() { tree.unpinAll(stack) }()
	defer func() { tree.wunlatchAll(stack) }()

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		defer popped.WUnlatch()
		stack = stack[:len(stack)-1]
		i, _ := popped.findKey(key)
		popped.InsertAt(i, rightKey, rightNod)
		//topOfStack.PrintNode()

		if popped.IsOverFlow(tree.degree) {
			rightNod, _, rightKey = popped.SplitNode((tree.degree) / 2)
			tree.pager.Unpin(popped, true)
			if rootLocked && popped.GetPageId() == tree.Root {
				leftNode := popped

				newRoot := tree.pager.NewInternalNode(leftNode.GetPageId())
				newRoot.InsertAt(0, rightKey, rightNod.(Pointer))
				tree.Root = newRoot.GetPageId()
				tree.pager.Unpin(newRoot, true)
				newRoot.WUnlatch()
			}
		} else {
			tree.pager.Unpin(popped, true)
			break
		}
	}
}

func (tree *BTree) InsertOrReplace(key common.Key, value interface{}) (isInserted bool) {
	i, stack := tree.FindAndGetStack(key, Insert)
	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1{
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	defer func() { tree.unpinAll(stack) }()
	defer func() { tree.wunlatchAll(stack) }()
	if i != nil {
		// top of stack is the leaf Node
		topOfStack := stack[len(stack)-1]
		leafNode := topOfStack.Node
		leafNode.setValueAt(topOfStack.Index, value)
		return false
	}

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		defer popped.WUnlatch()
		stack = stack[:len(stack)-1]
		i, _ := popped.findKey(key)
		popped.InsertAt(i, rightKey, rightNod)
		//topOfStack.PrintNode()

		if popped.IsOverFlow(tree.degree) {
			rightNod, _, rightKey = popped.SplitNode((tree.degree) / 2)
			tree.pager.Unpin(popped, true)
			if rootLocked && popped.GetPageId() == tree.Root {
				leftNode := popped

				newRoot := tree.pager.NewInternalNode(leftNode.GetPageId())
				newRoot.InsertAt(0, rightKey, rightNod.(Pointer))
				tree.Root = newRoot.GetPageId()
				tree.pager.Unpin(newRoot, true)
				newRoot.WUnlatch()
			}
		} else {
			tree.pager.Unpin(popped, true)
			break
		}
	}

	return true
}

func (tree *BTree) Find(key common.Key) interface{} {
	res, stack := tree.FindAndGetStack(key, Read)
	for _, pair := range stack {
		tree.pager.Unpin(pair.Node, false)
	}
	tree.runlatch(stack)
	return res
}

func (tree *BTree) FindSince(key common.Key) []interface{} {
	_, stack := tree.FindAndGetStack(key, Read)

	node := stack[len(stack)-1].Node
	vals := node.GetValues()
	res := vals[stack[len(stack)-1].Index:]
	for {
		p := node.GetRight()
		if p == 0 {
			node.RUnLatch()
			tree.pager.Unpin(node, false)
			break
		}
		old := node
		node = tree.pager.GetNode(p, Read)
		old.RUnLatch()
		tree.pager.Unpin(old, false)
		vals := node.GetValues()
		res = append(res, vals...)
	}

	return res
}

func (tree *BTree) Height() int {
	pager := tree.pager
	var currentNode Node = tree.pager.GetNode(tree.Root, Read)
	acc := 0
	for {
		if currentNode.IsLeaf() {
			currentNode.RUnLatch()
			return acc + 1
		} else {
			old := currentNode
			currentNode = pager.GetNode(currentNode.GetValueAt(0).(Pointer), Read)
			old.RUnLatch()
		}
		acc++
	}
}

func (tree *BTree) Count() int {
	tree.rootEntryLock.RLock()
	n := tree.GetRoot(Read)
	tree.rootEntryLock.RUnlock()
	for{
		if n.IsLeaf(){
			break
		}
		old := n
		n = tree.pager.GetNode(n.GetValueAt(0).(Pointer), Read)
		old.RUnLatch()
	}

	num := 0
	for{
		num += len(n.GetValues())
		r := n.GetRight()
		if r == 0{
			n.RUnLatch()
			break
		}
		old := n
		n = tree.pager.GetNode(r, Read)
		old.RUnLatch()
	}
	
	return num
}

func (tree BTree) Print() {
	pager := tree.pager
	queue := make([]Pointer, 0, 2)
	queue = append(queue, tree.Root)
	queue = append(queue, 0)
	for i := 0; i < len(queue); i++ {
		node := tree.pager.GetNode(queue[i], Read)
		if node != nil {
			node.RUnLatch()
		}
		if node != nil && node.IsLeaf() {
			break
		}
		if node == nil {
			queue = append(queue, 0)
			continue
		}

		pointers := make([]Pointer, 0)
		vals := node.GetValues()
		for _, val := range vals {
			pointers = append(pointers, val.(Pointer))
		}
		queue = append(queue, pointers...)
	}
	for _, n := range queue {
		if n != 0 {
			currNode := pager.GetNode(n, Read)
			defer currNode.RUnLatch()
			currNode.PrintNode()
		} else {
			fmt.Print("\n ### \n")
		}
	}
}

func (tree *BTree) Delete(key common.Key) bool {
	i, stack := tree.FindAndGetStack(key, Delete)
	rootLocked := false
	if len(stack) > 0 && stack[0].Index == -1{
		defer tree.rootEntryLock.Unlock()
		stack = stack[1:]
		rootLocked = true
	}
	defer func() { tree.unpinAll(stack) }()
	defer func() { tree.wunlatchAll(stack) }()
	if i == nil {
		return false
	}

	for len(stack) > 0 {
		popped := stack[len(stack)-1].Node
		defer popped.WUnlatch()
		stack = stack[:len(stack)-1]
		isPoppedDirty := false
		if popped.IsLeaf() {
			index, _ := popped.findKey(key)
			popped.DeleteAt(index)
			isPoppedDirty = true
		}

		if len(stack) == 0 {
			// if no parent left in stack(this is correct only if popped is root) it is done
			tree.pager.Unpin(popped, false) // NOTE: this one is tricky. But if root is dirty then previous turn in the loop should have already set it dirty
			return true
		}

		if popped.IsUnderFlow(tree.degree) {
			indexAtParent := stack[len(stack)-1].Index
			parent := stack[len(stack)-1].Node

			// get siblings
			// TODO: get write latch here
			var rightSibling, leftSibling, merged Node
			if indexAtParent > 0 {
				leftSibling = tree.pager.GetNode(parent.GetValueAt(indexAtParent - 1).(Pointer), Delete) //leftSibling = parent.Pointers[indexAtParent-1].(*InternalNode)
			}
			if indexAtParent+1 < (parent.Keylen() + 1) { // +1 is the length of pointers
				rightSibling = tree.pager.GetNode(parent.GetValueAt(indexAtParent + 1).(Pointer), Delete) //rightSibling = parent.Pointers[indexAtParent+1].(*InternalNode)
			}

			// try redistribute
			if rightSibling != nil &&
				((popped.IsLeaf() && rightSibling.Keylen() >= (tree.degree/2)+1) ||
					(!popped.IsLeaf() && rightSibling.Keylen()+1 > (tree.degree+1)/2)) { // TODO: second check is actually different for internal and leaf nodes since internal nodes have one more value than they have keys
				popped.Redistribute(rightSibling, parent)
				rightSibling.WUnlatch()
				// tree.pager.Unpin(parent, true) will be done by deferred unpinAll
				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(rightSibling, true)
				if leftSibling != nil {
					leftSibling.WUnlatch()
					tree.pager.Unpin(leftSibling, false)
				}
				return true
			} else if leftSibling != nil &&
				((popped.IsLeaf() && leftSibling.Keylen() >= (tree.degree/2)+1) ||
					(!popped.IsLeaf() && leftSibling.Keylen()+1 > (tree.degree+1)/2)) {
				leftSibling.Redistribute(popped, parent)
				leftSibling.WUnlatch()
				// tree.pager.Unpin(parent, true) will be done by deferred unpinAll
				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(leftSibling, true)
				if rightSibling != nil {
					rightSibling.WUnlatch()
					tree.pager.Unpin(rightSibling, false)
				}
				return true
			}

			// if redistribution is not valid merge
			if rightSibling != nil {
				popped.MergeNodes(rightSibling, parent)
				merged = popped
				rightSibling.WUnlatch()
				// tree.pager.Unpin(parent, true) will be done by deferred unpinAll
				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(rightSibling, true)
				if leftSibling != nil {
					leftSibling.WUnlatch()
					tree.pager.Unpin(leftSibling, false)
				}
			} else {
				if leftSibling == nil {
					if !popped.IsLeaf() {
						panic("Both siblings are null for an internal Node! This should not be possible except for root")
					}

					tree.pager.Unpin(popped, true)
					// TODO: may be log here? if it is a leaf node its both left and right nodes can be nil
					return true
				}
				leftSibling.MergeNodes(popped, parent)
				leftSibling.WUnlatch()
				merged = leftSibling

				// tree.pager.Unpin(parent, true) will be done by deferred unpinAll
				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(leftSibling, true)
				if rightSibling != nil {
					rightSibling.WUnlatch()
					tree.pager.Unpin(rightSibling, false)
				}
			}
			if rootLocked && parent.GetPageId() == tree.Root && parent.Keylen() == 0 {
				tree.Root = merged.GetPageId()
			}
		} else {
			tree.pager.Unpin(popped, isPoppedDirty)
			break
		}
	}

	return true
}

func (tree *BTree) findAndGetStack(node Node, key common.Key, stackIn []NodeIndexPair, mode TraverseMode) (value interface{}, stackOut []NodeIndexPair) {
	if node.IsLeaf() {
		i, found := node.findKey(key)
		stackOut = append(stackIn, NodeIndexPair{node, i})
		if !found {
			return nil, stackOut
		}
		return node.GetValueAt(i), stackOut
	} else {
		i, found := node.findKey(key)
		if found {
			i++
		}
		stackOut = append(stackIn, NodeIndexPair{node, i})
		pointer := node.GetValueAt(i).(Pointer)
		childNode := tree.pager.GetNode(pointer, mode)

		if mode == Read {
			node.RUnLatch()
		} else {
			// determine if child node is safe
			var safe bool
			if mode == Insert {
				safe = childNode.IsSafeForSplit(tree.degree)
			} else if mode == Delete {
				safe = childNode.IsSafeForMerge(tree.degree)
			}

			// if it is safe release all write locks above
			if safe {
				for len(stackOut) > 0 {
					n := stackOut[len(stackOut)-1].Node
					if stackOut[len(stackOut)-1].Index == -1{
						tree.rootEntryLock.Unlock()
					}else{
						n.WUnlatch()
						tree.pager.Unpin(n, false)
					}
					stackOut = stackOut[:len(stackOut)-1]
				}
			}
		}

		res, stackOut := tree.findAndGetStack(childNode, key, stackOut, mode)
		return res, stackOut
	}
}

func (tree *BTree) FindAndGetStack(key common.Key, mode TraverseMode) (value interface{}, stackOut []NodeIndexPair) {
	stack := []NodeIndexPair{}
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

func (tree *BTree) unpinAll(stack []NodeIndexPair) {
	for _, pair := range stack {
		tree.pager.Unpin(pair.Node, false)
	}
}

func (tree *BTree) wunlatchAll(stack []NodeIndexPair) {
	for _, pair := range stack {
		pair.Node.WUnlatch()
	}
}

func (tree *BTree) runlatch(stack []NodeIndexPair) {
	if len(stack) > 0 {
		stack[len(stack)-1].Node.RUnLatch()
	}
}
