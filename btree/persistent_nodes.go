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
	IsLeaf uint8
	KeyLen uint16
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
	dest := PersistentNodeHeader{
		IsLeaf: data[0],
		KeyLen: binary.BigEndian.Uint16(data[1:]),
		Right:  Pointer(binary.BigEndian.Uint64(data[3:])),
		Left:   Pointer(binary.BigEndian.Uint64(data[11:])),
	}

	return &dest
}

func WritePersistentNodeHeader(header *PersistentNodeHeader, dest []byte) {
	dest[0] = header.IsLeaf
	binary.BigEndian.PutUint16(dest[1:], header.KeyLen)
	binary.BigEndian.PutUint64(dest[3:], uint64(header.Right))
	binary.BigEndian.PutUint64(dest[11:], uint64(header.Left))
}

func (p *PersistentLeafNode) FindKey(key common.Key) (index int, found bool) {
	data := p.GetData()
	h := ReadPersistentNodeHeader(data)
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

func (p *PersistentLeafNode) setKeyAt(idx int, key common.Key) {
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
	return h.KeyLen == uint16(degree)
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

func (p *PersistentLeafNode) KeyLen() int {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen)
}

func (p *PersistentLeafNode) GetRight() Pointer {
	h := ReadPersistentNodeHeader(p.GetData())
	return h.Right
}

func (p *PersistentLeafNode) IsUnderFlow(degree int) bool {
	//return len(n.Values) < (degree)/2
	return (p.KeyLen()) < degree/2 // keylen + 1 is the values length
}

func (p *PersistentLeafNode) GetHeader() *PersistentNodeHeader {
	d := p.GetData()
	return ReadPersistentNodeHeader(d)
}

func (p *PersistentLeafNode) SetHeader(h *PersistentNodeHeader) {
	WritePersistentNodeHeader(h, p.GetData())
}

func (p *PersistentLeafNode) IsSafeForSplit(degree int) bool {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen) < degree-1
}

func (p *PersistentLeafNode) IsSafeForMerge(degree int) bool {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen) > (degree+1)/2
}

var _ Node = &PersistentInternalNode{}

type PersistentInternalNode struct {
	NodePage
	pager         Pager
	keySerializer KeySerializer
}

func (p *PersistentInternalNode) FindKey(key common.Key) (index int, found bool) {
	data := p.GetData()
	h := ReadPersistentNodeHeader(data)
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

func (p *PersistentInternalNode) KeyLen() int {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen)
}

func (p *PersistentInternalNode) GetRight() Pointer {
	panic("no right pointer for internal nodes for now")
}

func (p *PersistentInternalNode) IsUnderFlow(degree int) bool {
	return p.KeyLen() < degree/2
}

func (p *PersistentInternalNode) GetHeader() *PersistentNodeHeader {
	d := p.GetData()
	return ReadPersistentNodeHeader(d)
}

func (p *PersistentInternalNode) SetHeader(h *PersistentNodeHeader) {
	WritePersistentNodeHeader(h, p.GetData())
}

func (p *PersistentInternalNode) IsSafeForMerge(degree int) bool {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen) > (degree+1)/2
}

func (p *PersistentInternalNode) IsSafeForSplit(degree int) bool {
	h := ReadPersistentNodeHeader(p.GetData())
	return int(h.KeyLen) < degree-1
}
