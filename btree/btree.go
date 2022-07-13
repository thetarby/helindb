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
	"sort"
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
		i, _ := tree.FindKey(popped, key)
		popped.InsertAt(i, rightKey, rightNod)
		//topOfStack.PrintNode()

		if popped.IsOverFlow(tree.degree) {
			rightNod, _, rightKey = tree.splitNode(popped, (tree.degree) / 2)
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
		i, _ := tree.FindKey(popped, key)
		popped.InsertAt(i, rightKey, rightNod)
		//topOfStack.PrintNode()

		if popped.IsOverFlow(tree.degree) {
			rightNod, _, rightKey = tree.splitNode(popped, (tree.degree) / 2)
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
			index, _ := tree.FindKey(popped, key)
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
				tree.redistribute(popped, rightSibling, parent)
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
				tree.redistribute(leftSibling, popped, parent)
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
				tree.mergeNodes(popped, rightSibling, parent)
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
				tree.mergeNodes(leftSibling, popped, parent)
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

// findAndGetStack is used to recursively find the given key and it also passes a stack object recursively to
// keep the path it followed down to leaf node. value is nil when key does not exist.
func (tree *BTree) findAndGetStack(node Node, key common.Key, stackIn []NodeIndexPair, mode TraverseMode) (value interface{}, stackOut []NodeIndexPair) {
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

func (tree *BTree) mergeInternalNodes(p, rightNode, parent Node) {
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {}

	for ii := 0; ii < rightNode.Keylen()+1; ii++ {
		var k common.Key
		if ii == 0 {
			k = parent.GetKeyAt(i)
		} else {
			k = rightNode.GetKeyAt(ii - 1)
		}
		v := rightNode.GetValueAt(ii)
		p.InsertAt(p.Keylen(), k, v)
	}
	parent.DeleteAt(i)
}

func (tree *BTree) mergeLeafNodes(p, rightNode, parent Node) {
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {}

	for i := 0; i < rightNode.Keylen(); i++ {
		p.InsertAt(p.Keylen(), rightNode.GetKeyAt(i), rightNode.GetValueAt(i))
	}

	// TODO: destroy rightNode
	parent.DeleteAt(i)
	leftHeader := p.GetHeader()
	leftHeader.Right = rightNode.GetHeader().Right
	p.SetHeader(leftHeader)
}

func (tree *BTree) mergeNodes(p, rightNode, parent Node) {
	// TODO: this should free right node
	if p.IsLeaf(){
		tree.mergeLeafNodes(p, rightNode, parent)
	}else{
		tree.mergeInternalNodes(p, rightNode, parent)
	}
}

func (tree *BTree) redistributeInternalNodes(p, rightNode, parent Node) {
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {}
	
	numKeysAtLeft := (p.Keylen() + rightNode.Keylen()) / 2
	numKeysAtRight := (p.Keylen() + rightNode.Keylen()) - numKeysAtLeft

	for ii := 0; ii < rightNode.Keylen()+1; ii++ {
		var k common.Key
		if ii == 0 {
			k = parent.GetKeyAt(i)
		} else {
			k = rightNode.GetKeyAt(ii - 1)
		}
		v := rightNode.GetValueAt(ii)
		p.InsertAt(p.Keylen(), k, v)
	}
	
	rightNodeKeyLen := rightNode.Keylen()
	for i := 0; i < rightNodeKeyLen; i++ {
		rightNode.DeleteAt(i)
	}

	rightNode.setValueAt(0, p.GetValueAt(numKeysAtLeft+1))
	for i := numKeysAtLeft + 1; i < numKeysAtLeft+1+numKeysAtRight; i++ {
		k := p.GetKeyAt(i)
		v := p.GetValueAt(i + 1)
		rightNode.InsertAt(rightNode.Keylen(), k, v)
	}
	keyToParent := p.GetKeyAt(numKeysAtLeft)
	parent.setKeyAt(i, keyToParent)

	leftNodeKeyLen := p.Keylen()
	for i := numKeysAtLeft; i < leftNodeKeyLen; i++ {
		p.DeleteAt(i)
	}
}

func (tree *BTree) redistributeLeafNodes(p, rightNode, parent Node) {
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	}

	totalKeys := p.Keylen() + rightNode.Keylen()
	totalKeysInLeftAfterRedistribute := totalKeys / 2
	totalKeysInRightAfterRedistribute := totalKeys - totalKeysInLeftAfterRedistribute

	if p.Keylen() < totalKeysInLeftAfterRedistribute {
		// insert new keys to left
		diff := totalKeysInLeftAfterRedistribute - p.Keylen()
		for i := 0; i < diff; i++ {
			p.InsertAt(p.Keylen(), rightNode.GetKeyAt(0), rightNode.GetValueAt(0))
			rightNode.DeleteAt(0)
		}
	} else {
		diff := totalKeysInRightAfterRedistribute - rightNode.Keylen()
		for i := 0; i < diff; i++ {
			rightNode.InsertAt(0, p.GetKeyAt(p.Keylen()-1), p.GetValueAt(p.Keylen()-1))
			p.DeleteAt(p.Keylen() - 1)
		}
	}

	parent.setKeyAt(i, rightNode.GetKeyAt(0))
}

func (tree *BTree) redistribute(p, rightNode, parent Node) {
	if p.IsLeaf(){
		tree.redistributeLeafNodes(p, rightNode, parent)
	}else{
		tree.redistributeInternalNodes(p, rightNode, parent)
	}
}

func (tree *BTree) splitInternalNode(p Node, idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	// keyAtLeft is the last key in left node after split and keyAtRight is the key which is pushed up. it is actually
	// not in rightNode poor naming :(
	keyAtLeft = p.GetKeyAt(idx - 1)
	keyAtRight = p.GetKeyAt(idx)

	// create right node and insert into it
	// corresponding pointer is in the next index that is why +1
	rightNode := tree.pager.NewInternalNode(p.GetValueAt(idx + 1).(Pointer))
	defer tree.pager.Unpin(rightNode, true)
	defer rightNode.WUnlatch()

	for i := idx+1; i < p.Keylen(); i++ {
		rightNode.InsertAt(i-(idx+1), p.GetKeyAt(i), p.GetValueAt(i+1))
	}

	// delete from left node
	keylen := p.Keylen()
	for i := idx; i < keylen; i++ {
		p.DeleteAt(i)
	}

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (tree *BTree) splitLeafNode(p Node, idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	keyAtLeft = p.GetKeyAt(idx - 1)
	keyAtRight = p.GetKeyAt(idx)

	rightNode := tree.pager.NewLeafNode()
	defer tree.pager.Unpin(rightNode, true)
	defer rightNode.WUnlatch()

	for i := idx; i < p.Keylen(); i++ {
		rightNode.InsertAt(i-(idx), p.GetKeyAt(i), p.GetValueAt(i))
	}

	// delete from left node
	keylen := p.Keylen()
	for i := idx; i < keylen; i++ {
		p.DeleteAt(i)
	}

	leftHeader, rightHeader := p.GetHeader(), rightNode.GetHeader()
	rightHeader.Right = leftHeader.Right
	rightHeader.Left = p.GetPageId()
	leftHeader.Right = rightNode.GetPageId()
	p.SetHeader(leftHeader)
	rightNode.SetHeader(rightHeader)

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (tree *BTree) splitNode(p Node, idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	if p.IsLeaf(){
		return tree.splitLeafNode(p, idx)
	}else{
		return tree.splitInternalNode(p, idx)
	}
}

func (tree *BTree) FindKey(p Node, key common.Key) (index int, found bool) {
	h := p.GetHeader()
	i := sort.Search(int(h.KeyLen), func(i int) bool {
		return key.Less(p.GetKeyAt(i))
	})
	
	if i > 0 && !p.GetKeyAt(i-1).Less(key) {
		return i - 1, true
	}
	return i, false
}
