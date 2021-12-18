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
)

type BTree struct {
	degree int
	length int
	Root   Pointer
	pager  Pager
}

func NewBtreeWithPager(degree int, pager Pager) *BTree {
	l := pager.NewLeafNode()
	root := pager.NewInternalNode(l.GetPageId())
	defer pager.Unpin(root, true)
	defer pager.Unpin(l, true)

	return &BTree{
		degree: degree,
		length: 0,
		Root:   root.GetPageId(),
		pager:  pager,
	}
}

func (tree *BTree) GetRoot() Node {
	return tree.pager.GetNode(tree.Root)
}

func ConstructBtreeFromRootPointer(rootPage Pointer, degree int, pager Pager) *BTree {
	return &BTree{
		degree: degree,
		length: 0,
		Root:   rootPage,
		pager:  pager,
	}
}

func (tree *BTree) GetPager() Pager {
	return tree.pager
}

func (tree *BTree) Insert(key common.Key, value interface{}) {
	pager := tree.pager
	var stack = make([]NodeIndexPair, 0)
	var i interface{}
	root := tree.GetRoot()
	i, stack = tree.findAndGetStack(root, key, stack, Insert)
	defer tree.pager.Unpin(root, false)
	if i != nil {
		panic(fmt.Sprintf("key already exists:  %v", key))
	}

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		popped := tree.pager.GetNode(stack[len(stack)-1].Node)
		stack = stack[:len(stack)-1]
		i, _ := popped.findKey(key)
		popped.InsertAt(i, rightKey, rightNod)
		//topOfStack.PrintNode()

		if popped.IsOverFlow(tree.degree) {
			rightNod, _, rightKey = popped.SplitNode((tree.degree) / 2)
			tree.pager.Unpin(popped, true)
			tree.pager.Unpin(tree.pager.GetNode(rightNod.(Pointer)), true)
			//tree.pager.Unpin(popped, true)
			if popped.GetPageId() == tree.Root {
				leftNode := popped

				newRoot := pager.NewInternalNode(leftNode.GetPageId())
				newRoot.InsertAt(0, rightKey, rightNod.(Pointer))
				tree.Root = newRoot.GetPageId()
				tree.pager.Unpin(newRoot, true)
			}
		} else {
			tree.pager.Unpin(popped, true)
			break
		}
	}

	//for _, pair := range stack {
	//	tree.pager.Unpin(tree.pager.GetNode(pair.Node), false)
	//}
}

func (tree *BTree) InsertOrReplace(key common.Key, value interface{}) (isInserted bool) {
	pager := tree.pager
	var stack = make([]NodeIndexPair, 0)
	var i interface{}
	root := tree.GetRoot()
	i, stack = tree.findAndGetStack(root, key, stack, Insert)
	defer tree.pager.Unpin(root, false)
	if i != nil {
		// top of stack is the leaf Node
		topOfStack := stack[len(stack)-1]
		leafNode := tree.pager.GetNode(topOfStack.Node)
		leafNode.setValueAt(topOfStack.Index, value)
		return false
	}

	var rightNod = value
	var rightKey = key

	for len(stack) > 0 {
		topOfStack := tree.pager.GetNode(stack[len(stack)-1].Node)
		i, _ := topOfStack.findKey(key)
		topOfStack.InsertAt(i, rightKey, rightNod)

		popped := tree.pager.GetNode(stack[len(stack)-1].Node)
		stack = stack[:len(stack)-1]
		if popped.IsOverFlow(tree.degree) {
			rightNod, _, rightKey = popped.SplitNode((tree.degree) / 2)
			if popped.GetPageId() == tree.Root {
				leftNode := popped

				newRoot := pager.NewInternalNode(leftNode.GetPageId())
				newRoot.InsertAt(0, rightKey, rightNod.(Pointer))
				tree.Root = newRoot.GetPageId()
			}
		} else {
			break
		}
	}

	return true
}

func (tree *BTree) Find(key common.Key) interface{} {
	root := tree.GetRoot()
	res, stack := tree.findAndGetStack(root, key, []NodeIndexPair{}, Read)
	for _, pair := range stack {
		tree.pager.UnpinByPointer(pair.Node, false)
	}
	return res
}

func (tree *BTree) FindSince(key common.Key) []interface{} {
	root := tree.GetRoot()
	_, stack := tree.findAndGetStack(root, key, []NodeIndexPair{}, Read)

	node := tree.pager.GetNode(stack[len(stack)-1].Node)
	vals := node.GetValues()
	res := vals[stack[len(stack)-1].Index:]
	for {
		p := node.GetRight()
		if p == 0 {
			break
		}
		old := node
		node = tree.pager.GetNode(p)
		tree.pager.Unpin(old, false)
		vals := node.GetValues()
		res = append(res, vals...)
	}

	return res
}

func (tree *BTree) Height() int {
	pager := tree.pager
	var currentNode Node = tree.pager.GetNode(tree.Root)
	acc := 0
	for {
		if currentNode.IsLeaf() {
			return acc + 1
		} else {
			currentNode = pager.GetNode(currentNode.GetValueAt(0).(Pointer))
		}
		acc++
	}
}

func (tree BTree) Print() {
	pager := tree.pager
	queue := make([]Pointer, 0, 2)
	queue = append(queue, tree.Root)
	queue = append(queue, 0)
	for i := 0; i < len(queue); i++ {
		node := tree.pager.GetNode(queue[i])
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
			currNode := pager.GetNode(n)
			currNode.PrintNode()
		} else {
			fmt.Print("\n ### \n")
		}
	}
}

func (tree *BTree) Delete(key common.Key) bool {
	var stack = make([]NodeIndexPair, 0)
	var i interface{}
	root := tree.GetRoot()
	defer tree.pager.Unpin(root, false)
	i, stack = tree.findAndGetStack(root, key, stack, Delete)
	if i == nil {
		return false
	}

	for len(stack) > 0 {
		popped := tree.pager.GetNode(stack[len(stack)-1].Node)
		isPoppedDirty := false
		stack = stack[:len(stack)-1]
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
			parent := tree.pager.GetNode(stack[len(stack)-1].Node)

			// get siblings
			var rightSibling, leftSibling, merged Node
			if indexAtParent > 0 {
				leftSibling = tree.pager.GetNode(parent.GetValueAt(indexAtParent - 1).(Pointer)) //leftSibling = parent.Pointers[indexAtParent-1].(*InternalNode)
			}
			if indexAtParent+1 < (parent.Keylen() + 1) { // +1 is the length of pointers
				rightSibling = tree.pager.GetNode(parent.GetValueAt(indexAtParent + 1).(Pointer)) //rightSibling = parent.Pointers[indexAtParent+1].(*InternalNode)
			}

			//try redistribute
			if rightSibling != nil &&
				((popped.IsLeaf() && rightSibling.Keylen() >= (tree.degree/2)+1) ||
					(!popped.IsLeaf() && rightSibling.Keylen()+1 > (tree.degree+1)/2)) { // TODO: second check is actually different for internal and leaf nodes since internal nodes have one more value than they have keys
				popped.Redistribute(rightSibling, parent)

				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(rightSibling, true)
				tree.pager.Unpin(parent, true)
				return true
			} else if leftSibling != nil &&
				((popped.IsLeaf() && leftSibling.Keylen() >= (tree.degree/2)+1) ||
					(!popped.IsLeaf() && leftSibling.Keylen()+1 > (tree.degree+1)/2)) {
				leftSibling.Redistribute(popped, parent)

				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(leftSibling, true)
				tree.pager.Unpin(parent, true)
				return true
			}

			// if redistribution is not valid merge
			if rightSibling != nil {
				popped.MergeNodes(rightSibling, parent)
				merged = popped

				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(rightSibling, true)
				tree.pager.Unpin(parent, true)
			} else {
				if leftSibling == nil {
					if !popped.IsLeaf() {
						panic("Both siblings are null for an internal Node! This should not be possible except for root")
					}

					// TODO: may be log here? if it is a leaf node its both left and right nodes can be nil
					return true
				}
				leftSibling.MergeNodes(popped, parent)
				merged = leftSibling

				tree.pager.Unpin(popped, true)
				tree.pager.Unpin(leftSibling, true)
				tree.pager.Unpin(parent, true)
			}
			if parent.GetPageId() == tree.Root && parent.Keylen() == 0 {
				tree.Root = merged.GetPageId()
			}
		} else {
			tree.pager.Unpin(popped, isPoppedDirty)
			break
		}
	}

	//for _, pair := range stack {
	//	tree.pager.Unpin(tree.pager.GetNode(pair.Node), false)
	//}

	return true
}

// findAndGetStack is used to recursively find the given key and it also passes a stack object recursively to
// keep the path it followed down to leaf node. value is nil when key does not exist.
func (tree *BTree) findAndGetStack(node Node, key common.Key, stackIn []NodeIndexPair, mode TraverseMode) (value interface{}, stackOut []NodeIndexPair) {
	if node.IsLeaf() {
		i, found := node.findKey(key)
		stackOut = append(stackIn, NodeIndexPair{node.GetPageId(), i})
		if !found {
			return nil, stackOut
		}
		return node.GetValueAt(i), stackOut
	} else {
		i, found := node.findKey(key)
		if found {
			i++
		}
		stackOut = append(stackIn, NodeIndexPair{node.GetPageId(), i})
		pointer := node.GetValueAt(i).(Pointer)
		childNode := tree.pager.GetNode(pointer)

		// if mode == Insert{
		// 	if childNode.IsSafeForMerge(tree.degree){
		// 		for _, v := range st {

		// 		}
		// 		tree.pager.Unpin(node, false)
		// 	}
		// }

		defer tree.pager.Unpin(childNode, false)
		res, stackOut := tree.findAndGetStack(childNode, key, stackOut, mode)
		return res, stackOut
	}
}

func (tree *BTree) FindAndGetStack(key common.Key, mode TraverseMode) (value interface{}, stackOut []NodeIndexPair) {
	root := tree.GetRoot()
	defer tree.pager.Unpin(root, false)
	stack := []NodeIndexPair{}
	return tree.findAndGetStack(root, key, stack, mode)
}
