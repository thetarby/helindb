package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"helin/common"
	"sort"
)

type PersistentKey int64

func (p PersistentKey) Less(than common.Key) bool {
	return p < than.(PersistentKey)
}

type StringKey string

func (p StringKey) String() string {
	return string(p)
}

func (p StringKey) Less(than common.Key) bool {
	return p < than.(StringKey)
}

type SlotPointer struct {
	PageId  int64
	SlotIdx int16
}

// type Pointer int64

const (
	PersistentNodeHeaderSize = 3 + 2*NodePointerSize
	NodePointerSize          = 8 // Pointer is int64 which is 8 bytes
)

type PersistentNodeHeader struct {
	IsLeaf int8
	KeyLen int16
	Right  Pointer
	Left   Pointer
}

var _ Node = &PersistentLeafNode{}
type PersistentLeafNode struct {
	NodePage
	pager         Pager
	keySerializer KeySerializer
	valSerializer ValueSerializer
}

func ReadPersistentNodeHeader(data []byte) *PersistentNodeHeader {
	reader := bytes.NewReader(data)
	dest := PersistentNodeHeader{}
	binary.Read(reader, binary.BigEndian, &dest)
	return &dest
}

func WritePersistentNodeHeader(header *PersistentNodeHeader, dest []byte) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, header)
	CheckErr(err)
	copy(dest, buf.Bytes())
}

func (p *PersistentLeafNode) findKey(key common.Key) (index int, found bool) {
	data := p.GetData()
	h := ReadPersistentNodeHeader(data)
	//for i := 0; i < int(h.KeyLen) -1 ; i++ {
	//	currKey := p.GetKeyAt(i)
	//	nextKey := p.GetKeyAt(i+1)
	//	if currKey == key{
	//		return i, true
	//	}else if key.Less(nextKey){
	//		return i, false
	//	}
	//}

	//return int(h.KeyLen), false
	i := sort.Search(int(h.KeyLen), func(i int) bool {
		return key.Less(p.GetKeyAt(i))
	})

	if i > 0 && !p.GetKeyAt(i-1).Less(key) {
		return i - 1, true
	}
	return i, false
}

func (p *PersistentLeafNode) shiftKeyValueToRightAt(n int) {
	data := p.GetData()
	offset := n * (p.keySerializer.Size() + p.valSerializer.Size())
	copy(data[PersistentNodeHeaderSize+offset+p.keySerializer.Size()+p.valSerializer.Size():], data[PersistentNodeHeaderSize+offset:])
}

func (p *PersistentLeafNode) shiftKeyValueToLeftAt(n int) {
	// overrides the key-value pair at n-1
	if n < 1 {
		panic(fmt.Sprintf("index: %v cannot be shifted to left, it should be greater than 0", n))
	}
	data := p.GetData()
	offset := n * (p.keySerializer.Size() + p.valSerializer.Size())
	destOffset := (n - 1) * (p.keySerializer.Size() + p.valSerializer.Size())
	copy(data[PersistentNodeHeaderSize+destOffset:], data[PersistentNodeHeaderSize+offset:])
}

func (p *PersistentLeafNode) setKeyAt(idx int, key common.Key) { // TODO use persistentKey
	data := p.GetData()
	offset := idx * (p.keySerializer.Size() + p.valSerializer.Size())
	asByte, err := p.keySerializer.Serialize(key)
	CheckErr(err)
	copy(data[PersistentNodeHeaderSize+offset:], asByte)
}

func (p *PersistentLeafNode) setValueAt(idx int, val interface{}) {
	data := p.GetData()
	offset := (idx * (p.keySerializer.Size() + p.valSerializer.Size())) + p.keySerializer.Size()
	asByte, err := p.valSerializer.Serialize(val)
	CheckErr(err)
	copy(data[PersistentNodeHeaderSize+offset:], asByte)
}

func (p *PersistentLeafNode) GetKeyAt(idx int) common.Key {
	data := p.GetData()
	offset := idx * (p.keySerializer.Size() + p.valSerializer.Size())
	key, err := p.keySerializer.Deserialize(data[PersistentNodeHeaderSize+offset:])
	CheckErr(err)

	return key
}

func (p *PersistentLeafNode) GetValueAt(idx int) interface{} {
	data := p.GetData()
	offset := idx*(p.keySerializer.Size()+p.valSerializer.Size()) + p.keySerializer.Size()
	// TODO: direct byte array can be returned here since this method mostly called internally by methods that does not care about its content
	// return data[PersistentNodeHeaderSize+offset:PersistentNodeHeaderSize+offset:PersistentNodeHeaderSize+offset:PersistentNodeHeaderSize+offset+p.ValueSize]
	val, err := p.valSerializer.Deserialize(data[PersistentNodeHeaderSize+offset:])
	CheckErr(err)

	return val
}

func (p *PersistentLeafNode) GetValues() []interface{} {
	h := ReadPersistentNodeHeader(p.GetData())
	res := make([]interface{}, 0)
	for i := 0; i < int(h.KeyLen); i++ {
		res = append(res, p.GetValueAt(i))
	}
	return res
}

func (p *PersistentLeafNode) SplitNode(idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	pager := p.pager
	keyAtLeft = p.GetKeyAt(idx - 1)
	keyAtRight = p.GetKeyAt(idx)

	leftData := p.GetData()
	leftHeader := ReadPersistentNodeHeader(leftData)
	rightKeyLen := leftHeader.KeyLen - int16(idx)
	leftHeader.KeyLen = int16(idx)
	WritePersistentNodeHeader(leftHeader, leftData)
	offset := idx * (p.keySerializer.Size() + p.valSerializer.Size())

	rightNode := pager.NewLeafNode().(*PersistentLeafNode)
	defer pager.Unpin(rightNode, true)
	defer rightNode.WUnlatch()
	rightData := rightNode.GetData()
	copy(rightData[PersistentNodeHeaderSize:], leftData[PersistentNodeHeaderSize+offset:])
	rightHeader := ReadPersistentNodeHeader(rightData)
	rightHeader.KeyLen = rightKeyLen
	rightHeader.Right = leftHeader.Right
	rightHeader.Left = p.GetPageId()
	leftHeader.Right = rightNode.GetPageId()
	WritePersistentNodeHeader(rightHeader, rightData)
	WritePersistentNodeHeader(leftHeader, leftData)

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (p *PersistentLeafNode) PrintNode() {
	fmt.Printf("Node( ")
	h := ReadPersistentNodeHeader(p.GetData())
	for i := 0; i < int(h.KeyLen); i++ {
		fmt.Printf("%v | ", p.GetKeyAt(i))
	}
	fmt.Printf(")    ")
}

func (p *PersistentLeafNode) IsOverFlow(degree int) bool {
	h := ReadPersistentNodeHeader(p.GetData())
	return h.KeyLen == int16(degree)
}

func (p *PersistentLeafNode) InsertAt(index int, key common.Key, val interface{}) {
	// update header and increase key count
	h := ReadPersistentNodeHeader(p.GetData())
	h.KeyLen++
	WritePersistentNodeHeader(h, p.GetData())

	// shift pairs and insert new key, val pair
	p.shiftKeyValueToRightAt(index)
	p.setKeyAt(index, key)
	p.setValueAt(index, val)
}

func (p *PersistentLeafNode) IsLeaf() bool {
	return true
}

func (p *PersistentLeafNode) DeleteAt(index int) {
	// update header and decrease key count
	h := ReadPersistentNodeHeader(p.GetData())
	h.KeyLen--
	WritePersistentNodeHeader(h, p.GetData())

	p.shiftKeyValueToLeftAt(index + 1) // TODO: handle overflow. overlflow pages maybe?
}

func (p *PersistentLeafNode) Keylen() int {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen)
}

func (p *PersistentLeafNode) GetRight() Pointer {
	h := ReadPersistentNodeHeader(p.GetData())
	return h.Right
}

func (p *PersistentLeafNode) MergeNodes(rightNode Node, parent Node) {
	if parent.IsLeaf() {
		panic("parent node cannot be leaf")
	}
	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	}

	leftData := p.GetData()
	rightData := rightNode.(*PersistentLeafNode).GetData()
	leftHeader := ReadPersistentNodeHeader(leftData)
	rightHeader := ReadPersistentNodeHeader(rightData)

	endOfLeft := PersistentNodeHeaderSize + (int(leftHeader.KeyLen) * (p.valSerializer.Size() + p.keySerializer.Size()))
	copy(leftData[endOfLeft:], rightData[PersistentNodeHeaderSize:])

	// TODO: destroy rightNode
	parent.DeleteAt(i)
	leftHeader.KeyLen += rightHeader.KeyLen
	leftHeader.Right = rightHeader.Right
	WritePersistentNodeHeader(leftHeader, leftData)
}

func (p *PersistentLeafNode) Redistribute(rightNode Node, parent Node) {
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

func (p *PersistentLeafNode) IsUnderFlow(degree int) bool {
	//return len(n.Values) < (degree)/2
	return (p.Keylen()) < degree/2 // keylen + 1 is the values length
}

func (p *PersistentLeafNode) GetHeader() *PersistentNodeHeader {
	d := p.GetData()
	return ReadPersistentNodeHeader(d)
}

func (p *PersistentLeafNode) SetHeader(h *PersistentNodeHeader) {
	WritePersistentNodeHeader(h, p.GetData())
}

var _ Node = &PersistentInternalNode{}
type PersistentInternalNode struct {
	NodePage
	pager         Pager
	keySerializer KeySerializer
}

func NewPersistentInternalNode(firstPointer Pointer) *PersistentInternalNode {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	// create a new node
	// TODO: should use an adam akıllı pager
	node := PersistentInternalNode{NodePage: NewMemoryPage(1)}

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	// write first pointer
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, firstPointer)
	CheckErr(err)
	asByte := buf.Bytes()
	copy(data[PersistentNodeHeaderSize:], asByte)

	return &node

}

func (p *PersistentInternalNode) findKey(key common.Key) (index int, found bool) {
	data := p.GetData()
	h := ReadPersistentNodeHeader(data)
	//for i := 0; i < int(h.KeyLen) -1 ; i++ {
	//	currKey := p.GetKeyAt(i)
	//	nextKey := p.GetKeyAt(i+1)
	//	if currKey == key{
	//		return i, true
	//	}else if key.Less(nextKey){
	//		return i, false
	//	}
	//}

	//return int(h.KeyLen), false
	i := sort.Search(int(h.KeyLen), func(i int) bool {
		return key.Less(p.GetKeyAt(i))
	})

	if i > 0 && !p.GetKeyAt(i-1).Less(key) {
		return i - 1, true
	}
	return i, false
}

func (p *PersistentInternalNode) shiftKeyValueToRightAt(n int) {
	data := p.GetData()
	offset := n * (p.keySerializer.Size() + NodePointerSize)

	// in leaf nodes since there is one more pointer than keys additional pointer is stored right after the header
	// after that layout is same as leaf node. Rest of the page is like an array of key value pairs. In internal nodes
	// values are node pointers( Pointer )
	pairBeginningOffset := NodePointerSize + PersistentNodeHeaderSize
	copy(data[pairBeginningOffset+offset+p.keySerializer.Size()+NodePointerSize:], data[pairBeginningOffset+offset:])
}

func (p *PersistentInternalNode) shiftKeyValueToLeftAt(n int) {
	if n < 1 {
		panic(fmt.Sprintf("index: %v cannot be shifted to left, it should be greater than 0", n))
	}

	data := p.GetData()
	offset := n * (p.keySerializer.Size() + NodePointerSize)
	destOffset := (n - 1) * (p.keySerializer.Size() + NodePointerSize)

	// in leaf nodes since there is one more pointer than keys additional pointer is stored right after the header
	// after that layout is same as leaf node. Rest of the page is like an array of key value pairs. In internal nodes
	// values are node pointers( Pointer )
	pairBeginningOffset := NodePointerSize + PersistentNodeHeaderSize
	copy(data[pairBeginningOffset+destOffset:], data[pairBeginningOffset+offset:])
}

func (p *PersistentInternalNode) setKeyAt(idx int, key common.Key) {
	data := p.GetData()
	offset := idx * (p.keySerializer.Size() + NodePointerSize)
	pairBeginningOffset := PersistentNodeHeaderSize + NodePointerSize

	asByte, err := p.keySerializer.Serialize(key)
	CheckErr(err)
	copy(data[pairBeginningOffset+offset:], asByte)
}

func (p *PersistentInternalNode) setValueAt(idx int, val interface{}) {
	data := p.GetData()
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, val.(Pointer))
	CheckErr(err)
	asByte := buf.Bytes()

	if idx == 0 {
		// first pointer is located right after header
		copy(data[PersistentNodeHeaderSize:], asByte)
		return
	}
	offset := (idx-1)*(p.keySerializer.Size()+NodePointerSize) + p.keySerializer.Size()
	pairBeginningOffset := PersistentNodeHeaderSize + NodePointerSize
	copy(data[pairBeginningOffset+offset:], asByte)
}

func (p *PersistentInternalNode) GetKeyAt(idx int) common.Key {
	data := p.GetData()
	offset := idx * (p.keySerializer.Size() + NodePointerSize)
	pairBeginningOffset := PersistentNodeHeaderSize + NodePointerSize
	key, err := p.keySerializer.Deserialize(data[pairBeginningOffset+offset:])
	CheckErr(err)

	return key
}

func (p *PersistentInternalNode) GetValueAt(idx int) interface{} {
	data := p.GetData()
	var reader *bytes.Reader
	if idx == 0 {
		//first pointer is located right after header
		reader = bytes.NewReader(data[PersistentNodeHeaderSize:])
	} else {
		// first pointer is in a special position so, this offset is the offset after the pairs started
		// since first pointer is before pairs started in layout, it should not be calculated here. so idx - 1
		offset := (idx-1)*(p.keySerializer.Size()+NodePointerSize) + p.keySerializer.Size()
		pairBeginningOffset := PersistentNodeHeaderSize + NodePointerSize
		reader = bytes.NewReader(data[pairBeginningOffset+offset:])
	}
	var val Pointer
	err := binary.Read(reader, binary.BigEndian, &val)
	CheckErr(err)

	return val
}

func (p *PersistentInternalNode) GetValues() []interface{} {
	h := ReadPersistentNodeHeader(p.GetData())
	res := make([]interface{}, 0)
	res = append(res, p.GetValueAt(0)) // first pointer always exists
	for i := 0; i < int(h.KeyLen); i++ {
		res = append(res, p.GetValueAt(i+1)) // corresponding values are in the next index
	}
	return res
}

func (p *PersistentInternalNode) SplitNode(idx int) (right Pointer, keyAtLeft common.Key, keyAtRight common.Key) {
	pager := p.pager
	// keyAtLeft is the last key in right node after split and keyAtRight is the key which is pushed up. it is actually
	// not in rightNode poor naming :(
	keyAtLeft = p.GetKeyAt(idx - 1)
	keyAtRight = p.GetKeyAt(idx)

	// read page header and update key length. There is now idx number of remaining keys in left node. Others will be moved to
	// a new internal node
	leftData := p.GetData()
	leftHeader := ReadPersistentNodeHeader(leftData)
	rightKeyLen := leftHeader.KeyLen - int16(idx) - 1
	leftHeader.KeyLen = int16(idx)
	WritePersistentNodeHeader(leftHeader, leftData)
	offset := (idx + 1) * (p.keySerializer.Size() + NodePointerSize)
	pairBeginningOffset := PersistentNodeHeaderSize + NodePointerSize

	// corresponding pointer is in the next index that is why +1
	rightNode := pager.NewInternalNode(p.GetValueAt(idx + 1).(Pointer)).(*PersistentInternalNode)
	defer pager.Unpin(rightNode, true)
	defer rightNode.WUnlatch()
	rightData := rightNode.GetData()
	copy(rightData[pairBeginningOffset:], leftData[pairBeginningOffset+offset:])
	rightHeader := ReadPersistentNodeHeader(rightData)
	rightHeader.KeyLen = rightKeyLen
	WritePersistentNodeHeader(rightHeader, rightData)

	return rightNode.GetPageId(), keyAtLeft, keyAtRight
}

func (p *PersistentInternalNode) PrintNode() {
	fmt.Printf("Node( ")
	h := ReadPersistentNodeHeader(p.GetData())
	for i := 0; i < int(h.KeyLen); i++ {
		fmt.Printf("%v | ", p.GetKeyAt(i))
	}
	fmt.Printf(")    ")
}

func (p *PersistentInternalNode) IsOverFlow(degree int) bool {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen) == degree
}

func (p *PersistentInternalNode) InsertAt(index int, key common.Key, val interface{}) {
	h := ReadPersistentNodeHeader(p.GetData())
	h.KeyLen++
	WritePersistentNodeHeader(h, p.GetData())

	p.shiftKeyValueToRightAt(index)
	p.setKeyAt(index, key)
	p.setValueAt(index+1, val)
}

func (p *PersistentInternalNode) IsLeaf() bool {
	return false
}

func (p *PersistentInternalNode) DeleteAt(index int) {
	h := ReadPersistentNodeHeader(p.GetData())
	h.KeyLen--
	WritePersistentNodeHeader(h, p.GetData())

	p.shiftKeyValueToLeftAt(index + 1)
}

func (p *PersistentInternalNode) Keylen() int {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen)
}

func (p *PersistentInternalNode) GetRight() Pointer {
	panic("no right pointer for internal nodes for now")
}

func (p *PersistentInternalNode) MergeNodes(rightNode Node, parent Node) {
	//if parent.IsLeaf(){
	//	panic("parent node cannot be leaf")
	//}
	//var i int
	//for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	//}
	//
	//// init needed variables
	//leftData := p.GetData()
	//rightData := rightNode.(*PersistentInternalNode).GetData()
	//parentData := parent.(*PersistentInternalNode).GetData()
	//leftHeader := ReadPersistentNodeHeader(leftData)
	//rightHeader := ReadPersistentNodeHeader(rightData)
	//
	//// define offsets
	//endOfLeft := PersistentNodeHeaderSize + NodePointerSize + ((leftHeader.KeyLen) * (NodePointerSize + KeySize))
	//rightNodeFirstPointerOffset := PersistentNodeHeaderSize
	//parentNodePushedDownKeyOffset := PersistentNodeHeaderSize + NodePointerSize + i * (NodePointerSize + KeySize)
	//
	////push down the key in the parent
	//copy(leftData[endOfLeft:], parentData[parentNodePushedDownKeyOffset: parentNodePushedDownKeyOffset+KeySize])
	//
	//// below two can be merged but this is more expressive. firstly append first pointer of the right node and then append the rest
	//copy(leftData[endOfLeft + KeySize:], rightData[rightNodeFirstPointerOffset:rightNodeFirstPointerOffset+NodePointerSize])
	//copy(leftData[endOfLeft + KeySize + NodePointerSize:], rightData[rightNodeFirstPointerOffset+NodePointerSize:])
	//
	//// TODO: destroy rightNode
	//parent.DeleteAt(i)
	//leftHeader.KeyLen += rightHeader.KeyLen + 1 // +1 is the pushed down key
	//WritePersistentNodeHeader(leftHeader, leftData)

	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	}

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

func (p *PersistentInternalNode) Redistribute(rightNode Node, parent Node) {
	//var i int
	//for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {}
	//
	//leftData := p.GetData()
	//rightData := rightNode.(*PersistentInternalNode).GetData()
	//parentData := parent.(*PersistentInternalNode).GetData()
	//leftHeader := ReadPersistentNodeHeader(leftData)
	//rightHeader := ReadPersistentNodeHeader(rightData)
	//mergedData := make([]byte, 0)
	//mergedData = append(mergedData, leftData[PersistentNodeHeaderSize:PersistentNodeHeaderSize + NodePointerSize + p.Keylen() * (KeySize + NodePointerSize)]...)
	//mergedData = append(mergedData, parentData[PersistentNodeHeaderSize + NodePointerSize + i * (KeySize + NodePointerSize):KeySize + PersistentNodeHeaderSize + NodePointerSize + i * (KeySize + NodePointerSize)]...)
	//mergedData = append(mergedData, rightData[PersistentNodeHeaderSize:PersistentNodeHeaderSize + NodePointerSize + rightNode.Keylen() * (KeySize + NodePointerSize)]...)
	//
	//totalKeys := p.Keylen() + rightNode.Keylen()
	//totalKeysInLeftAfterRedistribute := totalKeys / 2
	//totalKeysInRightAfterRedistribute := totalKeys - totalKeysInLeftAfterRedistribute
	//copy(leftData[PersistentNodeHeaderSize:], mergedData[:NodePointerSize + totalKeysInLeftAfterRedistribute * (KeySize + NodePointerSize)])
	//copy(rightData[PersistentNodeHeaderSize:], mergedData[NodePointerSize + KeySize + totalKeysInLeftAfterRedistribute * (KeySize + NodePointerSize):])
	//parentKeyLocOffset := PersistentNodeHeaderSize + KeySize + i * (KeySize + NodePointerSize)
	//copy(parentData[parentKeyLocOffset:parentKeyLocOffset+KeySize], mergedData[NodePointerSize + totalKeysInLeftAfterRedistribute * (KeySize + NodePointerSize): NodePointerSize + KeySize + totalKeysInLeftAfterRedistribute * (KeySize + NodePointerSize)])
	//leftHeader.KeyLen  = int16(totalKeysInLeftAfterRedistribute)
	//rightHeader.KeyLen = int16(totalKeysInRightAfterRedistribute)
	//WritePersistentNodeHeader(leftHeader, leftData)
	//WritePersistentNodeHeader(rightHeader, rightData)

	var i int
	for i = 0; parent.GetValueAt(i).(Pointer) != p.GetPageId(); i++ {
	}
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
	rightData := rightNode.(*PersistentInternalNode).GetData()
	rightHeader := ReadPersistentNodeHeader(rightData)
	rightHeader.KeyLen = 0
	WritePersistentNodeHeader(rightHeader, rightData)

	rightNode.setValueAt(0, p.GetValueAt(numKeysAtLeft+1))
	for i := numKeysAtLeft + 1; i < numKeysAtLeft+1+numKeysAtRight; i++ {
		k := p.GetKeyAt(i)
		v := p.GetValueAt(i + 1)
		rightNode.InsertAt(rightNode.Keylen(), k, v)
	}
	keyToParent := p.GetKeyAt(numKeysAtLeft)
	parent.setKeyAt(i, keyToParent)

	leftHeader := ReadPersistentNodeHeader(p.GetData())
	leftHeader.KeyLen = int16(numKeysAtLeft)
	WritePersistentNodeHeader(leftHeader, p.GetData())
}

func (p *PersistentInternalNode) IsUnderFlow(degree int) bool {
	return p.Keylen() < degree/2
}

func (p *PersistentInternalNode) GetHeader() *PersistentNodeHeader {
	d := p.GetData()
	return ReadPersistentNodeHeader(d)
}

func (p *PersistentInternalNode) SetHeader(h *PersistentNodeHeader) {
	WritePersistentNodeHeader(h, p.GetData())
}

func (n *PersistentLeafNode) IsSafeForMerge(degree int) bool {
	h := ReadPersistentNodeHeader(n.GetData())
	return int(h.KeyLen) > (degree+1)/2
}

func (n *PersistentInternalNode) IsSafeForMerge(degree int) bool {
	h := ReadPersistentNodeHeader(n.GetData())
	return int(h.KeyLen) > (degree+1)/2
}

func (n *PersistentLeafNode) IsSafeForSplit(degree int) bool {
	h := ReadPersistentNodeHeader(n.GetData())
	return int(h.KeyLen) < degree-1
}

func (n *PersistentInternalNode) IsSafeForSplit(degree int) bool {
	h := ReadPersistentNodeHeader(n.GetData())
	return int(h.KeyLen) < degree-1
}
