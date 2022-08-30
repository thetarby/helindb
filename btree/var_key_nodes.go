package btree

import (
	"encoding/binary"
	"fmt"
	"helin/common"
)

// pointer size is the byte size of a page pointer
var pointerSize = binary.Size(Pointer(0))

// maxPayloadSize is the maximum size of sizeof(key) + sizeof(value) + varintsizeof(len(key))
var maxPayloadSize = 128

// maxRequiredSize is maximum size that could be required to insert a payload at the maxPayloadSize. this is bigger than maxPayloadSize
// because it also has overflow page's pointer, the length of the payload as varint and the SLOT_ARR_ENTRY_SIZE 
var maxRequiredSize = maxPayloadSize + pointerSize + binary.MaxVarintLen16 + SLOT_ARR_ENTRY_SIZE

var _ Node = &VarKeyLeafNode{}

// VarKeyLeafNode is a leaf node implementation which supports variable sized keys
type VarKeyLeafNode struct {
	p             SlottedPage
	keySerializer KeySerializer
	valSerializer ValueSerializer
	pager         Pager
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
	b := n.getAt(idx + 1)
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

	n.insertAt(index+1, buf[:len(valb)+len(keyb)+nn])

	h := n.GetHeader()
	h.KeyLen++
	n.SetHeader(h)
}

func (n *VarKeyLeafNode) DeleteAt(index int) {
	n.p.DeleteAt(index + 1)
	n.p.Vacuum()
	h := n.GetHeader()
	h.KeyLen--
	n.SetHeader(h)
}

func (n *VarKeyLeafNode) GetValueAt(idx int) interface{} {
	b := n.getAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	val, err := n.valSerializer.Deserialize(b[nn+int(keySize):])
	CheckErr(err)

	return val
}

func (n *VarKeyLeafNode) setKeyAt(idx int, key common.Key) {
	b := n.getAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	valb := b[nn+int(keySize):]

	newKb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(newKb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(newKb)))
	copy(buf[nn:], newKb)
	copy(buf[nn+len(newKb):], valb)

	n.setAt(idx+1, b, buf[:len(newKb)+len(valb)+nn])
}

func (n *VarKeyLeafNode) setValueAt(idx int, val interface{}) {
	b := n.getAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	keyb := b[nn : nn+int(keySize)]

	newValb, err := n.valSerializer.Serialize(val)
	CheckErr(err)

	buf := make([]byte, int(keySize)+len(newValb)+4)
	nn = binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], newValb)

	n.setAt(idx+1, b, buf[:len(keyb)+len(newValb)+nn])
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

// IsOverFlow returns true when either key length size is equal to degree or empty space in underlying page
// is less than maxRequiredSize. Because then it is not possible to be sure that next key will not throw an
// not enough space error. (This implementation might change later)
func (n *VarKeyLeafNode) IsOverFlow(degree int) bool {
	// return n.p.GetFreeSpaceIdeal() < (maxPayloadSize + pointerSize + maxvarint(for slot entry))
	h := n.GetHeader()
	es := n.p.EmptySpace()
	return int(h.KeyLen) == degree || es < (maxRequiredSize)
}

func (n *VarKeyLeafNode) IsSafeForMerge(degree int) bool {
	// return n.p.FilledSize() - (maxPayloadSize + pointerSize) >= (n.p.Cap() / 2)
	fit := n.p.Cap() / maxRequiredSize
	threshold := fit / 4
	return n.Keylen() > threshold
}

func (n *VarKeyLeafNode) IsSafeForSplit(degree int) bool {
	// return n.p.GetFreeSpaceIdeal() - (maxPayloadSize + pointerSize + maxvarint(for slot entry)) >= (maxPayloadSize + pointerSize + maxvarint(for slot entry))
	h := n.GetHeader()
	return int(h.KeyLen) < degree-1 && n.p.EmptySpace() - (maxRequiredSize) >= (maxRequiredSize)
}

// IsUnderFlow returns true when key length is less than 1/4 of total keys that could fit with maxPayloadSize.
// NOTE: this could also be anded with a check of utilized space in underlying page. That would make sense because keys
// might be so short and IsUnderFlow might become true so late. It is not a threat to correctness but could result
// in a less balanced tree.
func (n *VarKeyLeafNode) IsUnderFlow(degree int) bool {
	// return n.p.FilledSize() < (n.p.Cap() / 2)
	fit := n.p.Cap() / maxRequiredSize
	threshold := fit / 4
	return n.Keylen() < threshold
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

func (n *VarKeyLeafNode) setAt(idx int, old, data []byte) {
	if len(old) > maxPayloadSize {
		// TODO: free page
		// b := n.p.GetAt(idx)
		// ptr := DeserializePointer(b[maxPayloadSize:])
		// n.pager.Free(ptr)
	}

	if len(data) > maxPayloadSize {
		rest := data[maxPayloadSize:]
		init := data[:maxPayloadSize]
		overflowPage := n.pager.CreatePage()
		defer n.pager.UnpinByPointer(overflowPage.GetPageId(), true)

		newData := make([]byte, len(init) + pointerSize)
		nc := copy(newData, init)
		overflowPage.GetPageId().Serialize(newData[nc:])

		newRest := make([]byte, len(rest)+binary.MaxVarintLen64)
		nc = binary.PutUvarint(newRest, uint64(len(rest)))
		copy(newRest[nc:], rest)

		copy(overflowPage.GetData(), newRest)

		err := n.p.SetAt(idx, newData)
		CheckErr(err)
		return
	}

	err := n.p.SetAt(idx, data)
	CheckErr(err)
}

func (n *VarKeyLeafNode) insertAt(idx int, data []byte) {
	// TODO: optimize this method
	if len(data) > maxPayloadSize {
		rest := data[maxPayloadSize:]
		init := data[:maxPayloadSize]
		overflowPage := n.pager.CreatePage()
		defer n.pager.UnpinByPointer(overflowPage.GetPageId(), true)

		newData := make([]byte, len(init) + pointerSize)
		nc := copy(newData, init)
		overflowPage.GetPageId().Serialize(newData[nc:])

		newRest := make([]byte, len(rest)+binary.MaxVarintLen64)
		nc = binary.PutUvarint(newRest, uint64(len(rest)))
		copy(newRest[nc:], rest)

		copy(overflowPage.GetData(), newRest)

		if len(newData) > maxPayloadSize + 8 {
			panic("whatttt")
		}
		err := n.p.InsertAt(idx, newData)
		h := n.p.GetHeader()
		if false{
			println(h)
		}
		CheckErr(err)
		return
	}

	err := n.p.InsertAt(idx, data)
	CheckErr(err)
}

func (n *VarKeyLeafNode) getAt(idx int) []byte {
	// TODO: optimize this method
	b := n.p.GetAt(idx)

	if len(b) > maxPayloadSize {
		of := n.pager.GetPage(DeserializePointer(b[len(b)-pointerSize:]))
		if of == nil {
			panic(fmt.Sprintf("page id not found: %v, idx: %v, len: %v", DeserializePointer(b[len(b)-pointerSize:]), idx, len(b)))
		}
		defer n.pager.UnpinByPointer(of.GetPageId(), false)

		d := of.GetData()
		size, readBytes := binary.Uvarint(d)

		res := make([]byte, len(b)-pointerSize+int(size))
		copied := copy(res, b[:len(b)-pointerSize])
		copy(res[copied:], d[readBytes:int(size)+readBytes])

		return res
	}

	return b
}

var _ Node = &VarKeyInternalNode{}

// VarKeyInternalNode is an internal node implementation which supports variable sized keys
type VarKeyInternalNode struct {
	p             SlottedPage
	keySerializer KeySerializer
	pager         Pager
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

	buf := make([]byte, len(keyb)+pointerSize+4)

	nn := binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	val.(Pointer).Serialize(buf[nn+len(keyb):])

	n.insertAt(index+2, buf[:pointerSize+len(keyb)+nn])

	h := n.GetHeader()
	h.KeyLen++
	n.SetHeader(h)
}

func (n *VarKeyInternalNode) DeleteAt(index int) {
	n.p.DeleteAt(index + 2)
	n.p.Vacuum()
	h := n.GetHeader()
	h.KeyLen--
	n.SetHeader(h)
}

func (n *VarKeyInternalNode) GetValueAt(idx int) interface{} {
	if idx == 0 {
		b := n.getAt(1)
		p := binary.BigEndian.Uint64(b)
		return Pointer(p)
	}

	b := n.getAt(idx + 1)
	keySize, nn := binary.Uvarint(b)

	return DeserializePointer(b[nn+int(keySize):])
}

func (n *VarKeyInternalNode) GetKeyAt(idx int) common.Key {
	b := n.getAt(idx + 2)
	keySize, nn := binary.Uvarint(b)

	key, err := n.keySerializer.Deserialize(b[nn : nn+int(keySize)])
	CheckErr(err)

	return key
}

func (n *VarKeyInternalNode) setKeyAt(idx int, key common.Key) {
	b := n.getAt(idx + 2)
	keySize, nn := binary.Uvarint(b)

	valb := b[nn+int(keySize):]

	newKb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(newKb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(newKb)))
	copy(buf[nn:], newKb)
	copy(buf[nn+len(newKb):], valb)

	n.setAt(idx+2, b, buf[:len(newKb)+len(valb)+nn])
}

func (n *VarKeyInternalNode) setValueAt(idx int, val interface{}) {
	valb := val.(Pointer).Bytes()
	if idx == 0 {
		n.setAt(1, n.getAt(0), valb)
		return
	}

	b := n.getAt(idx + 1)
	keySize, nn := binary.Uvarint(b)
	keyb := b[nn : nn+int(keySize)]

	buf := make([]byte, len(keyb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], valb)

	n.setAt(idx+1, b, buf[:len(keyb)+len(valb)+nn])
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
	// return n.p.GetFreeSpaceIdeal() < maxPayloadSize + pointerSize
	h := n.GetHeader()
	return int(h.KeyLen) == degree || n.p.EmptySpace() < (maxRequiredSize)
}

func (n *VarKeyInternalNode) IsSafeForMerge(degree int) bool {
	// return n.p.FilledSize() - (maxPayloadSize + pointerSize) >= (n.p.Cap() / 2)
	fit := n.p.Cap() / maxRequiredSize
	threshold := fit / 4
	return n.Keylen() > threshold
}

func (n *VarKeyInternalNode) IsSafeForSplit(degree int) bool {
	// return n.p.FilledSize() > ((n.p.Cap() / 2) + maxPayloadSize + pointerSize)
	h := n.GetHeader()
	return int(h.KeyLen) < degree-1 && n.p.EmptySpace() - (maxRequiredSize) >= (maxRequiredSize)
}

func (n *VarKeyInternalNode) IsUnderFlow(degree int) bool {
	// return n.p.FilledSize() < (n.p.Cap() / 2)
	fit := n.p.Cap() / maxRequiredSize
	threshold := fit / 4
	return n.Keylen() < threshold
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

func (n *VarKeyInternalNode) setAt(idx int, old, data []byte) {
	if len(old) > maxPayloadSize {
		// TODO: free page
		// b := n.p.GetAt(idx)
		// ptr := DeserializePointer(b[maxPayloadSize:])
		// n.pager.Free(ptr)
	}

	if len(data) > maxPayloadSize {
		rest := data[maxPayloadSize:]
		init := data[:maxPayloadSize]
		overflowPage := n.pager.CreatePage()
		defer n.pager.UnpinByPointer(overflowPage.GetPageId(), true)

		newData := make([]byte, len(init) + pointerSize)
		nc := copy(newData, init)
		overflowPage.GetPageId().Serialize(newData[nc:])

		newRest := make([]byte, len(rest)+binary.MaxVarintLen64)
		nc = binary.PutUvarint(newRest, uint64(len(rest)))
		copy(newRest[nc:], rest)

		copy(overflowPage.GetData(), newRest)

		err := n.p.SetAt(idx, newData)
		CheckErr(err)
		return
	}

	err := n.p.SetAt(idx, data)
	CheckErr(err)
}

func (n *VarKeyInternalNode) insertAt(idx int, data []byte) {
	if len(data) > maxPayloadSize {
		rest := data[maxPayloadSize:]
		init := data[:maxPayloadSize]
		overflowPage := n.pager.CreatePage()
		defer n.pager.UnpinByPointer(overflowPage.GetPageId(), true)

		newData := make([]byte, len(init) + pointerSize)
		nc := copy(newData, init)
		overflowPage.GetPageId().Serialize(newData[nc:])

		newRest := make([]byte, len(rest)+binary.MaxVarintLen64)
		nc = binary.PutUvarint(newRest, uint64(len(rest)))
		copy(newRest[nc:], rest)

		copy(overflowPage.GetData(), newRest)

		err := n.p.InsertAt(idx, newData)
		CheckErr(err)
		return
	}

	err := n.p.InsertAt(idx, data)
	CheckErr(err)
}

func (n *VarKeyInternalNode) getAt(idx int) []byte {
	b := n.p.GetAt(idx)

	if len(b) > maxPayloadSize {
		of := n.pager.GetPage(DeserializePointer(b[len(b)-pointerSize:]))
		defer n.pager.UnpinByPointer(of.GetPageId(), false)
		d := of.GetData()
		size, readBytes := binary.Uvarint(d)

		res := make([]byte, len(b)-pointerSize+int(size))
		copied := copy(res, b[:len(b)-pointerSize])
		copy(res[copied:], d[readBytes:int(size)+readBytes])

		return res
	}

	return b
}