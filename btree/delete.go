package btree

type DeletableNode interface {
	shiftKeyValueToLeftAt(n int)
	appendKey(key Key)
	appendNode(node Node)
	
	Keylen() int
	GetRight() Pointer
	GetLeft() Pointer
	MergeNodes(rightNode Node, parent Node)
	Redistribute(rightNode_ Node, parent_ Node)
	IsUnderFlow(degree int) bool
	TruncateAfterValueAt(idx int)
}

func MergeNodes(d DeletableNode, rightNode Node, parent Node)  {
	dAsNode := d.(Node)

	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != dAsNode.GetPageId(); i++ {
	}
	//d.appendKey(parent.GetKeyAt(i))
	//d.appendNode(rightNode)
	for ii := 0; ii < rightNode.Keylen()+1; ii++ {
		var k Key
		if ii == 0{ k = parent.GetKeyAt(i)} else { k = rightNode.GetKeyAt(i-1) }
		v := rightNode.GetValueAt(ii)
		dAsNode.InsertAt(dAsNode.Keylen(), k, v)
	}
	parent.DeleteAt(i)
}

func Distribute(d DeletableNode, rightNode Node, parent Node)  {
	dAsNode := d.(Node)

	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != dAsNode.GetPageId(); i++ {
	}
	for ii := 0; ii < rightNode.Keylen()+1; ii++ {
		var k Key
		if ii == 0{ k = parent.GetKeyAt(i)} else { k = rightNode.GetKeyAt(i-1) }
		v := rightNode.GetValueAt(ii)
		dAsNode.InsertAt(dAsNode.Keylen(), k, v)
	}
	numKeysAtLeft := (dAsNode.Keylen() + rightNode.Keylen()) / 2
	numKeysAtRight := (dAsNode.Keylen() + rightNode.Keylen()) - numKeysAtLeft

	for i := numKeysAtLeft+1; i < numKeysAtLeft+1+numKeysAtRight; i++ {
		k:=dAsNode.GetKeyAt(i)
		v:=dAsNode.GetValueAt(i)
		rightNode.InsertAt(rightNode.Keylen(), k, v)
	}
	keyToParent := dAsNode.GetKeyAt(numKeysAtLeft)
	parent.setKeyAt(i, keyToParent)

	d.TruncateAfterValueAt(numKeysAtLeft)
}

func (n *InternalNode) Keylen() int {
	return len(n.Keys)
}

func (n *InternalNode) GetRight() Pointer {
	panic("no right pointer for internal nodes for now")
}

func (n *InternalNode) GetLeft() Pointer {
	panic("no left pointer for internal nodes for now")
}

func (n *InternalNode) IsUnderFlow(degree int) bool {
	return len(n.Keys) < (degree/2)
}

func (n *LeafNode) Keylen() int {
	return len(n.Keys)
}

func (n *LeafNode) GetRight() Pointer {
	if n.Right == nil {
		return -1 // TODO: -1 should never point to a real page
	}
	return n.Right.GetPageId()
}

func (n *LeafNode) GetLeft() Pointer {
	if n.Left == nil {
		return -1 // TODO: -1 should never point to a real page
	}
	return n.Left.GetPageId()
}

func (n *LeafNode) IsUnderFlow(degree int) bool {
	return len(n.Values) < (degree)/2
}

func (n *LeafNode) Redistribute(rightNode_ Node, parent_ Node) {
	rightNode := rightNode_.(*LeafNode)
	parent := parent_.(*InternalNode)

	var i int
	for i := 0; parent.GetValueAt(i).(Pointer) != n.GetPageId(); i++ {
	}

	keys := append(n.Keys, rightNode.Keys...)
	vals := append(n.Values, rightNode.Values...)

	n.Keys = keys[:len(keys)/2]
	n.Values = vals[:len(vals)/2]
	rightNode.Keys = keys[len(keys)/2:]
	rightNode.Values = vals[len(vals)/2:]

	parent.Keys[i] = rightNode.Keys[0]
}

func (n *InternalNode) Redistribute(rightNode_ Node, parent_ Node) {
	rightNode := rightNode_.(*InternalNode)
	parent := parent_.(*InternalNode)

	var i int
	for i := 0; parent.GetValueAt(i).(Pointer) != n.GetPageId(); i++ {
	}

	keys := append(n.Keys, parent.Keys[i])
	keys = append(keys, rightNode.Keys...)

	vals := append(n.Pointers, rightNode.Pointers...)

	numKeysInLeft := len(keys) / 2
	n.Keys = keys[:numKeysInLeft]
	n.Pointers = vals[:1+numKeysInLeft]
	rightNode.Keys = keys[numKeysInLeft+1:]
	rightNode.Pointers = vals[numKeysInLeft+1:]

	parent.Keys[i] = keys[numKeysInLeft]
}

func (n *LeafNode) MergeNodes(rightNode_ Node, parent_ Node) {
	rightNode := rightNode_.(*LeafNode)
	parent := parent_.(*InternalNode)
	var i int
	for i = 0; parent.Pointers[i] != n.GetPageId(); i++ { // TODO: burdaki i scoped muş yani for içinde i:=0 diye init etmişim diğer branchleri kontrol et doru mu bunun çalışöıyor olması lazım
	}

	keys := append(n.Keys, rightNode.Keys...)
	parent.DeleteAt(i)
	vals := append(n.Values, rightNode.Values...)

	n.Keys = keys
	n.Values = vals

	// delete at shifts to left by one
	parent.Pointers[i] = n.GetPageId()

	n.Right = rightNode.Right
}

func (n *InternalNode) MergeNodes(rightNode_ Node, parent_ Node) {
	rightNode := rightNode_.(*InternalNode)
	parent := parent_.(*InternalNode)
	var i int
	for i := 0; parent.Pointers[i] != n.GetPageId(); i++ {
	}

	keys := append(n.Keys, parent.Keys[i])
	keys = append(keys, rightNode.Keys...)
	parent.DeleteAt(i)
	pointers := append(n.Pointers, rightNode.Pointers...)

	n.Keys = keys
	n.Pointers = pointers

	// delete at shifts to left by one
	parent.Pointers[i] = n.GetPageId()
}

func (n *InternalNode) DeleteAt(index int) {
	n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
	n.Pointers = append(n.Pointers[:index], n.Pointers[index+1:]...)
}

func (n *LeafNode) DeleteAt(index int) {
	n.Keys = append(n.Keys[:index], n.Keys[index+1:]...)
	n.Values = append(n.Values[:index], n.Values[index+1:]...)
}
