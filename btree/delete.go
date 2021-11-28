package btree

import "helin/common"

type DeletableNode interface {
	shiftKeyValueToLeftAt(n int)
	appendKey(key common.Key)
	appendNode(node Node)

	Keylen() int
	GetRight() Pointer
	GetLeft() Pointer
	MergeNodes(rightNode Node, parent Node)
	Redistribute(rightNode_ Node, parent_ Node)
	IsUnderFlow(degree int) bool
	TruncateAfterValueAt(idx int)
}

func MergeNodes(d DeletableNode, rightNode Node, parent Node) {
	dAsNode := d.(Node)

	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != dAsNode.GetPageId(); i++ {
	}
	//d.appendKey(parent.GetKeyAt(i))
	//d.appendNode(rightNode)
	for ii := 0; ii < rightNode.Keylen()+1; ii++ {
		var k common.Key
		if ii == 0 {
			k = parent.GetKeyAt(i)
		} else {
			k = rightNode.GetKeyAt(i - 1)
		}
		v := rightNode.GetValueAt(ii)
		dAsNode.InsertAt(dAsNode.Keylen(), k, v)
	}
	parent.DeleteAt(i)
}

func Distribute(d DeletableNode, rightNode Node, parent Node) {
	dAsNode := d.(Node)

	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != dAsNode.GetPageId(); i++ {
	}
	for ii := 0; ii < rightNode.Keylen()+1; ii++ {
		var k common.Key
		if ii == 0 {
			k = parent.GetKeyAt(i)
		} else {
			k = rightNode.GetKeyAt(i - 1)
		}
		v := rightNode.GetValueAt(ii)
		dAsNode.InsertAt(dAsNode.Keylen(), k, v)
	}
	numKeysAtLeft := (dAsNode.Keylen() + rightNode.Keylen()) / 2
	numKeysAtRight := (dAsNode.Keylen() + rightNode.Keylen()) - numKeysAtLeft

	for i := numKeysAtLeft + 1; i < numKeysAtLeft+1+numKeysAtRight; i++ {
		k := dAsNode.GetKeyAt(i)
		v := dAsNode.GetValueAt(i)
		rightNode.InsertAt(rightNode.Keylen(), k, v)
	}
	keyToParent := dAsNode.GetKeyAt(numKeysAtLeft)
	parent.setKeyAt(i, keyToParent)

	d.TruncateAfterValueAt(numKeysAtLeft)
}
