package btree

import (
	"encoding/binary"
	"fmt"
	"helin/common"
)

var pointerSize = binary.Size(Pointer(0))

var _ Node = &VarKeyLeafNode{}

// VarKeyLeafNode is a leaf node implementation which supports variable sized keys
type VarKeyLeafNode struct {
	p             SlottedPage
	keySerializer KeySerializer
	valSerializer ValueSerializer
}

func (n *VarKeyLeafNode) GetPageId() Pointer {
	return n.p.GetPageId()
}

func (n *VarKeyLeafNode) GetHeader() *PersistentNodeHeader {
	return ReadPersistentNodeHeader(n.p.GetAt(0))
}

func (n *VarKeyLeafNode) SetHeader(h *PersistentNodeHeader) {
	arr := make([]byte, PersistentNodeHeaderSize)
	WritePersistentNodeHeader(h, arr)
	if (n.p.GetHeader().SlotArrSize) == 0 {
		n.p.InsertAt(0, arr)
	} else {
		n.p.SetAt(0, arr)
	}
}

func (n *VarKeyLeafNode) IsLeaf() bool {
	return true
}

func (n *VarKeyLeafNode) GetRight() Pointer {
	return n.GetHeader().Right
}

func (n *VarKeyLeafNode) GetKeyAt(idx int) common.Key {
	b := n.p.GetAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	key, err := n.keySerializer.Deserialize(b[nn : nn+int(keySize)])
	CheckErr(err)

	return key
}

func (n *VarKeyLeafNode) InsertAt(index int, key common.Key, val interface{}) {
	valb, err := n.valSerializer.Serialize(val)
	CheckErr(err)

	keyb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(valb)+len(keyb)+4)
	nn := binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], valb)

	err = n.p.InsertAt(index+1, buf[:len(valb)+len(keyb)+nn])
	CheckErr(err)

	h := n.GetHeader()
	h.KeyLen++
	n.SetHeader(h)
}

func (n *VarKeyLeafNode) DeleteAt(index int) {
	n.p.DeleteAt(index+1)
	n.p.Vacuum()
	h := n.GetHeader()
	h.KeyLen--
	n.SetHeader(h)
}

func (n *VarKeyLeafNode) GetValueAt(idx int) interface{} {
	b := n.p.GetAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	val, err := n.valSerializer.Deserialize(b[nn+int(keySize):])
	CheckErr(err)

	return val
}

func (n *VarKeyLeafNode) setKeyAt(idx int, key common.Key) {
	b := n.p.GetAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	valb := b[nn+int(keySize):]

	newKb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(newKb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(newKb)))
	copy(buf[nn:], newKb)
	copy(buf[nn+len(newKb):], valb)

	err = n.p.SetAt(idx+1, buf[:len(newKb)+len(valb)+nn])
	CheckErr(err)
}

func (n *VarKeyLeafNode) setValueAt(idx int, val interface{}) {
	b := n.p.GetAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	keyb := b[nn:nn+int(keySize)]

	newValb, err := n.valSerializer.Serialize(val)
	CheckErr(err)

	buf := make([]byte, int(keySize) + len(newValb) + 4)
	nn = binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], newValb)

	err = n.p.SetAt(idx+1, buf[:len(keyb)+len(newValb)+nn])
	CheckErr(err)
}

func (n *VarKeyLeafNode) GetValues() []interface{} {
	keylen := n.Keylen()
	vals := make([]interface{}, keylen)
	for i := 0; i < keylen; i++ {
		v := n.GetValueAt(i)
		vals[i] = v
	}
	return vals
}

func (n *VarKeyLeafNode) IsOverFlow(degree int) bool {
	h := n.GetHeader()
	return int(h.KeyLen) == degree
}

func (n *VarKeyLeafNode) IsSafeForMerge(degree int) bool {
	h := n.GetHeader()
	return int(h.KeyLen) > (degree+1)/2
}

func (n *VarKeyLeafNode) IsSafeForSplit(degree int) bool {
	h := n.GetHeader()
	return int(h.KeyLen) < degree-1
}

func (n *VarKeyLeafNode) IsUnderFlow(degree int) bool {
	return n.Keylen() < degree/2
}

func (n *VarKeyLeafNode) Keylen() int {
	return int(n.GetHeader().KeyLen)
}

func (n *VarKeyLeafNode) PrintNode() {
	fmt.Printf("LeafNode( ")
	for i := 0; i < int(n.Keylen()); i++ {
		fmt.Printf("%v | ", n.GetKeyAt(i))
	}
	fmt.Printf(")\n")
}

func (n *VarKeyLeafNode) RLatch() {
	n.p.RLatch()
}

func (n *VarKeyLeafNode) RUnLatch() {
	n.p.RUnLatch()
}

func (n *VarKeyLeafNode) WLatch() {
	n.p.WLatch()
}

func (n *VarKeyLeafNode) WUnlatch() {
	n.p.WUnlatch()
}

var _ Node = &VarKeyInternalNode{}

// VarKeyInternalNode is an internal node implementation which supports variable sized keys
type VarKeyInternalNode struct {
	p             SlottedPage
	keySerializer KeySerializer
}

func (n *VarKeyInternalNode) GetPageId() Pointer {
	return n.p.GetPageId()
}

func (n *VarKeyInternalNode) GetHeader() *PersistentNodeHeader {
	return ReadPersistentNodeHeader(n.p.GetAt(0))
}

func (n *VarKeyInternalNode) SetHeader(h *PersistentNodeHeader) {
	arr := make([]byte, PersistentNodeHeaderSize)
	WritePersistentNodeHeader(h, arr)
	if (n.p.GetHeader().SlotArrSize) == 0 {
		n.p.InsertAt(0, arr)
	} else {
		n.p.SetAt(0, arr)
	}
}

func (n *VarKeyInternalNode) IsLeaf() bool {
	return false
}

func (n *VarKeyInternalNode) GetRight() Pointer {
	panic("no right pointer for internal nodes")
}

func (n *VarKeyInternalNode) InsertAt(index int, key common.Key, val interface{}) {
	keyb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(keyb)+ pointerSize +4)

	nn := binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	val.(Pointer).Serialize(buf[nn+len(keyb):])

	err = n.p.InsertAt(index+2, buf[:pointerSize+len(keyb)+nn])
	CheckErr(err)

	h := n.GetHeader()
	h.KeyLen++
	n.SetHeader(h)
}

func (n *VarKeyInternalNode) DeleteAt(index int) {
	n.p.DeleteAt(index+2)
	n.p.Vacuum()
	h := n.GetHeader()
	h.KeyLen--
	n.SetHeader(h)
}

func (n *VarKeyInternalNode) GetValueAt(idx int) interface{} {
	if idx == 0 {
		b := n.p.GetAt(1)
		p := binary.BigEndian.Uint64(b)
		return Pointer(p)
	}

	b := n.p.GetAt(idx + 1)
	keySize, nn := binary.Uvarint(b)
 
	return DeserializePointer(b[nn+int(keySize):])
}

func (n *VarKeyInternalNode) GetKeyAt(idx int) common.Key {
	b := n.p.GetAt(idx + 2)
	keySize, nn := binary.Uvarint(b)

	key, err := n.keySerializer.Deserialize(b[nn : nn+int(keySize)])
	CheckErr(err)

	return key
}

func (n *VarKeyInternalNode) setKeyAt(idx int, key common.Key) {
	b := n.p.GetAt(idx + 2)
	keySize, nn := binary.Uvarint(b)

	valb := b[nn+int(keySize):]

	newKb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(newKb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(newKb)))
	copy(buf[nn:], newKb)
	copy(buf[nn+len(newKb):], valb)

	err = n.p.SetAt(idx+2, buf[:len(newKb)+len(valb)+nn])
	CheckErr(err)
}

func (n *VarKeyInternalNode) setValueAt(idx int, val interface{}) {
	valb := val.(Pointer).Bytes()
	if idx == 0 {
		err := n.p.SetAt(1, valb)
		CheckErr(err)
		return
	}

	b := n.p.GetAt(idx + 1)
	keySize, nn := binary.Uvarint(b)
	keyb := b[nn:nn+int(keySize)]

	buf := make([]byte, len(keyb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], valb)

	err := n.p.SetAt(idx+1, buf[:len(keyb)+len(valb)+nn])
	CheckErr(err)
}

func (n *VarKeyInternalNode) GetValues() []interface{} {
	keylen := n.Keylen()
	vals := make([]interface{}, keylen+1)
	for i := 0; i < keylen+1; i++ {
		v := n.GetValueAt(i)
		vals[i] = v
	}
	return vals
}

func (n *VarKeyInternalNode) IsOverFlow(degree int) bool {
	h := n.GetHeader()
	return int(h.KeyLen) == degree
}

func (n *VarKeyInternalNode) IsSafeForMerge(degree int) bool {
	h := n.GetHeader()
	return int(h.KeyLen) > (degree+1)/2
}

func (n *VarKeyInternalNode) IsSafeForSplit(degree int) bool {
	h := n.GetHeader()
	return int(h.KeyLen) < degree-1
}

func (n *VarKeyInternalNode) IsUnderFlow(degree int) bool {
	return n.Keylen() < degree/2
}

func (n *VarKeyInternalNode) Keylen() int {
	return int(n.GetHeader().KeyLen)
}

func (n *VarKeyInternalNode) PrintNode() {
	fmt.Printf("Node( ")
	for i := 0; i < n.Keylen(); i++ {
		fmt.Printf("%v | ", n.GetKeyAt(i))
	}
	fmt.Printf(")    ")
}

func (n *VarKeyInternalNode) RLatch() {
	n.p.RLatch()
}

func (n *VarKeyInternalNode) RUnLatch() {
	n.p.RUnLatch()
}

func (n *VarKeyInternalNode) WLatch() {
	n.p.WLatch()
}

func (n *VarKeyInternalNode) WUnlatch() {
	n.p.WUnlatch()
}
