package btree

import (
	"encoding/binary"
	"fmt"
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
	"log"
	"strings"
)

/*
	This file contains leaf and internal node implementations both of which wraps a BPage and uses it
	slightly differently.

	Each node optionally has an overflow which is pointed by its header. Nodes manage freeing of overflow structures
	when there is no payload that overflows. Nodes also manage creating overflows when first overflow occurs.

	Nodes store first maxPayloadSize of the payload in node itself and the rest is kept in the overflow. Nodes also
	manage deleting, updating, inserting and fetching those parts of the payload in the overflow structure.
*/

// pointer size is the byte size of a page pointer
var pointerSize = binary.Size(Pointer(0))

// maxPayloadSize is the maximum size of sizeof(key) + sizeof(value) + varintsizeof(len(key))
var maxPayloadSize = 390

// MaxRequiredSize is maximum size that could be required to insert a payload at the maxPayloadSize. this is bigger than maxPayloadSize
// because it also has overflow page's pointer, the length of the payload as varint and the SlotArrEntrySize
var MaxRequiredSize = maxPayloadSize + pointerSize + binary.MaxVarintLen16 + pages.SlotArrEntrySize // maxPayloadSize+13

var _ node = &VarKeyLeafNode{}

// VarKeyLeafNode is a leaf node implementation which supports variable sized keys
type VarKeyLeafNode struct {
	p             BPage
	keySerializer KeySerializer
	valSerializer ValueSerializer
	pager         *Pager2
}

func (n *VarKeyLeafNode) FillFactor() int {
	return fitted(n.p)
}

func (n *VarKeyLeafNode) GetPageId() Pointer {
	return n.p.GetPageId()
}

func (n *VarKeyLeafNode) GetHeader() *PersistentNodeHeader {
	return ReadPersistentNodeHeader(n.p.GetAt(0))
}

func (n *VarKeyLeafNode) SetHeader(txn transaction.Transaction, h *PersistentNodeHeader) {
	arr := make([]byte, PersistentNodeHeaderSize)
	WritePersistentNodeHeader(h, arr)
	if n.p.Count() == 0 {
		CheckErr(n.p.InsertAt(txn, 0, arr))
	} else {
		CheckErr(n.p.SetAt(txn, 0, arr))
	}
}

func (n *VarKeyLeafNode) IsLeaf() bool {
	return true
}

func (n *VarKeyLeafNode) GetRight() Pointer {
	return n.GetHeader().Right
}

func (n *VarKeyLeafNode) GetKeyAt(txn transaction.Transaction, idx int) common.Key {
	// b := getAt(&n.p, idx+1)
	b := getKeyAt(txn, n.pager, n, n.p, idx+1)
	keySize, nn := binary.Uvarint(b)

	key, err := n.keySerializer.Deserialize(b[nn : nn+int(keySize)])
	CheckErr(err)

	return key
}

func (n *VarKeyLeafNode) InsertAt(txn transaction.Transaction, index int, key common.Key, val interface{}) {
	valb, err := n.valSerializer.Serialize(val)
	CheckErr(err)

	keyb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(valb)+len(keyb)+4)
	nn := binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], valb)

	//if err := n.p.InsertAt(txn, index+1, buf[:len(valb)+len(keyb)+nn]); err != nil {
	//	panic(err)
	//}
	insertAt(txn, n, n.pager, n.p, index+1, buf[:len(valb)+len(keyb)+nn])

	h := n.GetHeader()
	h.KeyLen++
	n.SetHeader(txn, h)
}

func (n *VarKeyLeafNode) DeleteAt(txn transaction.Transaction, index int) {
	//deleteAt(txn, &n.p, index+1)
	deleteAt(txn, n, n.pager, n.p, index+1)
	// n.p.Vacuum() // TODO: why vacumm, no vacuum?
	h := n.GetHeader()
	h.KeyLen--
	n.SetHeader(txn, h)
}

func (n *VarKeyLeafNode) GetValueAt(txn transaction.Transaction, idx int) interface{} {
	//b := getAt(&n.p, idx+1)
	b := getAt(txn, n.pager, n, n.p, idx+1)
	keySize, nn := binary.Uvarint(b)

	val, err := n.valSerializer.Deserialize(b[nn+int(keySize):])
	CheckErr(err)

	return val
}

func (n *VarKeyLeafNode) SetKeyAt(txn transaction.Transaction, idx int, key common.Key) {
	// b := getAt(&n.p, idx+1)
	b := getAt(txn, n.pager, n, n.p, idx+1)
	keySize, nn := binary.Uvarint(b)

	valb := b[nn+int(keySize):]

	newKb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(newKb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(newKb)))
	copy(buf[nn:], newKb)
	copy(buf[nn+len(newKb):], valb)

	// setAt(txn, &n.p, idx+1, buf[:len(newKb)+len(valb)+nn])
	setAt(txn, n, n.pager, n.p, idx+1, b, buf[:len(newKb)+len(valb)+nn])
}

func (n *VarKeyLeafNode) SetValueAt(txn transaction.Transaction, idx int, val interface{}) {
	// b := getAt(&n.p, idx+1)
	b := getAt(txn, n.pager, n, n.p, idx+1)
	keySize, nn := binary.Uvarint(b)

	keyb := b[nn : nn+int(keySize)]

	newValb, err := n.valSerializer.Serialize(val)
	CheckErr(err)

	buf := make([]byte, int(keySize)+len(newValb)+4)
	nn = binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], newValb)

	// setAt(txn, &n.p, idx+1, buf[:len(keyb)+len(newValb)+nn])
	setAt(txn, n, n.pager, n.p, idx+1, b, buf[:len(keyb)+len(newValb)+nn])
}

func (n *VarKeyLeafNode) GetValues(txn transaction.Transaction) []interface{} {
	keylen := n.KeyLen()
	vals := make([]interface{}, keylen)
	for i := 0; i < keylen; i++ {
		v := n.GetValueAt(txn, i)
		vals[i] = v
	}
	return vals
}

func (n *VarKeyLeafNode) KeyLen() int {
	return int(n.GetHeader().KeyLen)
}

func (n *VarKeyLeafNode) PrintNode(txn transaction.Transaction) {
	b := strings.Builder{}
	b.WriteString("LeafNode( ")
	for i := 0; i < n.KeyLen(); i++ {
		b.WriteString(fmt.Sprintf("%v | ", n.GetKeyAt(txn, i)))
	}
	b.WriteString(")\n")

	log.Println(b.String())
}

var _ node = &VarKeyInternalNode{}

// VarKeyInternalNode is an internal node implementation which supports variable sized keys
type VarKeyInternalNode struct {
	p             BPage
	keySerializer KeySerializer
	pager         *Pager2
}

func (n *VarKeyInternalNode) FillFactor() int {
	return fitted(n.p)
}

func (n *VarKeyInternalNode) GetPageId() Pointer {
	return n.p.GetPageId()
}

func (n *VarKeyInternalNode) GetHeader() *PersistentNodeHeader {
	return ReadPersistentNodeHeader(n.p.GetAt(0))
}

func (n *VarKeyInternalNode) SetHeader(txn transaction.Transaction, h *PersistentNodeHeader) {
	arr := make([]byte, PersistentNodeHeaderSize)
	WritePersistentNodeHeader(h, arr)
	if n.p.Count() == 0 {
		// set header operation cannot return error because there must be space for header all the time
		// hence panicking here makes sense since it means underlying code is not correct.
		CheckErr(n.p.InsertAt(txn, 0, arr))
	} else {
		CheckErr(n.p.SetAt(txn, 0, arr))
	}
}

func (n *VarKeyInternalNode) IsLeaf() bool {
	return false
}

func (n *VarKeyInternalNode) GetRight() Pointer {
	panic("no right pointer for internal nodes")
}

func (n *VarKeyInternalNode) InsertAt(txn transaction.Transaction, index int, key common.Key, val interface{}) {
	keyb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(keyb)+pointerSize+4)

	nn := binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	val.(Pointer).Serialize(buf[nn+len(keyb):])

	insertAt(txn, n, n.pager, n.p, index+2, buf[:pointerSize+len(keyb)+nn])

	h := n.GetHeader()
	h.KeyLen++
	n.SetHeader(txn, h)
}

func (n *VarKeyInternalNode) DeleteAt(txn transaction.Transaction, index int) {
	// deleteAt(txn, &n.p, index+2)
	deleteAt(txn, n, n.pager, n.p, index+2)
	// n.p.Vacuum()
	h := n.GetHeader()
	h.KeyLen--
	n.SetHeader(txn, h)
}

func (n *VarKeyInternalNode) GetValueAt(txn transaction.Transaction, idx int) interface{} {
	if idx == 0 {
		// b := getAt(&n.p, 1)
		b := getAt(txn, n.pager, n, n.p, 1)
		p := binary.BigEndian.Uint64(b)
		return Pointer(p)
	}

	// b := getAt(&n.p, idx+1)
	b := getAt(txn, n.pager, n, n.p, idx+1)
	keySize, nn := binary.Uvarint(b)

	return DeserializePointer(b[nn+int(keySize):])
}

func (n *VarKeyInternalNode) GetKeyAt(txn transaction.Transaction, idx int) common.Key {
	// TODO: this can be optimized, there might not no need to read from heap if key is fit in node, same holds for SetKeyAt
	// b := getAt(&n.p, idx+2)
	b := getKeyAt(txn, n.pager, n, n.p, idx+2)
	keySize, nn := binary.Uvarint(b)

	key, err := n.keySerializer.Deserialize(b[nn : nn+int(keySize)])
	CheckErr(err)

	return key
}

func (n *VarKeyInternalNode) SetKeyAt(txn transaction.Transaction, idx int, key common.Key) {
	// b := getAt(&n.p, idx+2)
	b := getAt(txn, n.pager, n, n.p, idx+2)
	keySize, nn := binary.Uvarint(b)

	valb := b[nn+int(keySize):]

	newKb, err := n.keySerializer.Serialize(key)
	CheckErr(err)

	buf := make([]byte, len(newKb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(newKb)))
	copy(buf[nn:], newKb)
	copy(buf[nn+len(newKb):], valb)

	//setAt(txn, &n.p, idx+2, buf[:len(newKb)+len(valb)+nn])
	setAt(txn, n, n.pager, n.p, idx+2, b, buf[:len(newKb)+len(valb)+nn])
}

func (n *VarKeyInternalNode) SetValueAt(txn transaction.Transaction, idx int, val interface{}) {
	valb := val.(Pointer).Bytes()
	if idx == 0 {
		//setAt(txn, &n.p, 1, valb)
		setAt(txn, n, n.pager, n.p, 1, getAt(txn, n.pager, n, n.p, 0), valb)
		return
	}

	// b := getAt(&n.p, idx+1)
	b := getAt(txn, n.pager, n, n.p, idx+1)
	keySize, nn := binary.Uvarint(b)
	keyb := b[nn : nn+int(keySize)]

	buf := make([]byte, len(keyb)+len(valb)+4)
	nn = binary.PutUvarint(buf, uint64(len(keyb)))
	copy(buf[nn:], keyb)
	copy(buf[nn+len(keyb):], valb)

	//setAt(txn, &n.p, idx+1, buf[:len(keyb)+len(valb)+nn])
	setAt(txn, n, n.pager, n.p, idx+1, b, buf[:len(keyb)+len(valb)+nn])
}

func (n *VarKeyInternalNode) GetValues(txn transaction.Transaction) []interface{} {
	keylen := n.KeyLen()
	vals := make([]interface{}, keylen+1)
	for i := 0; i < keylen+1; i++ {
		v := n.GetValueAt(txn, i)
		vals[i] = v
	}
	return vals
}

func (n *VarKeyInternalNode) KeyLen() int {
	return int(n.GetHeader().KeyLen)
}

func (n *VarKeyInternalNode) PrintNode(txn transaction.Transaction) {
	b := strings.Builder{}
	b.WriteString("node( ")
	for i := 0; i < n.KeyLen(); i++ {
		b.WriteString(fmt.Sprintf("%v | ", n.GetKeyAt(txn, i)))
	}
	b.WriteString(")    ")

	log.Println(b.String())
}

func setAt(txn transaction.Transaction, n node, pager *Pager2, p BPage, idx int, old, data []byte) {
	// if old value is larger than maxPayloadSize first delete its rest part from overflow
	if len(old) > maxPayloadSize {
		h := n.GetHeader()
		overflow, err := pager.GetOverflowReleaser(h.Overflow)
		CheckErr(err)

		init := p.GetAt(idx)
		slotIdx := int(DeserializePointer(init[maxPayloadSize : maxPayloadSize+pointerSize])) //todo
		err = overflow.DeleteAt(txn, slotIdx)
		CheckErr(err)

		c, err := overflow.Count(txn)
		CheckErr(err)
		if c == 0 {
			err := pager.FreeOverflow(txn, Pointer(overflow.GetPageId()))
			CheckErr(err)
			h.Overflow = 0
			n.SetHeader(txn, h)
		}
	}

	// if new value is larger than maxPayloadSize store first bytes in the node and the rest in overflow
	if len(data) > maxPayloadSize {
		// create overflow if not exists, else get it
		var overflow OverflowReleaser
		var err error
		h := n.GetHeader()
		if h.Overflow == 0 {
			overflow, err = pager.CreateOverflow(txn)
			CheckErr(err)
			h.Overflow = Pointer(overflow.GetPageId())
			n.SetHeader(txn, h)
		} else {
			overflow, err = pager.GetOverflowReleaser(h.Overflow)
			CheckErr(err)
		}

		// split data into two parts so that first maxPayloadSize bytes will be in the node and the rest will be spilled
		// onto overflow
		rest := data[maxPayloadSize:]
		init := data[:maxPayloadSize]

		i, err := overflow.Insert(txn, rest)
		CheckErr(err)

		// TODO: no need to add overflow pageID here. it is fixed and in the node header
		newData := make([]byte, len(init)+pointerSize)
		nc := copy(newData, init)
		Pointer(i).Serialize(newData[nc:])

		err = p.SetAt(txn, idx, newData)
		CheckErr(err)
	} else {
		err := p.SetAt(txn, idx, data)
		CheckErr(err)
	}
}

func insertAt(txn transaction.Transaction, n node, pager *Pager2, p BPage, idx int, data []byte) {
	if len(data) > maxPayloadSize {
		// TODO: handle overflow pages on transaction rollbacks.
		var overflow OverflowReleaser
		var err error
		h := n.GetHeader()
		if h.Overflow == 0 {
			overflow, err = pager.CreateOverflow(txn)
			CheckErr(err)

			h.Overflow = Pointer(overflow.GetPageId())
			n.SetHeader(txn, h)
		} else {
			overflow, err = pager.GetOverflowReleaser(h.Overflow)
			CheckErr(err)
		}

		rest := data[maxPayloadSize:]
		init := data[:maxPayloadSize]

		i, err := overflow.Insert(txn, rest)
		CheckErr(err)

		newData := make([]byte, len(init)+pointerSize)
		nc := copy(newData, init)
		Pointer(i).Serialize(newData[nc:]) // TODO i is not uint64

		err = p.InsertAt(txn, idx, newData)
		CheckErr(err)
	} else {
		err := p.InsertAt(txn, idx, data)
		CheckErr(err)
	}
}

func deleteAt(txn transaction.Transaction, n node, pager *Pager2, p BPage, idx int) {
	old := p.GetAt(idx)
	if len(old) > maxPayloadSize {
		h := n.GetHeader()
		overflow, err := pager.GetOverflowReleaser(h.Overflow)
		CheckErr(err)

		slotIdx := int(DeserializePointer(old[len(old)-pointerSize:])) //todo
		err = overflow.DeleteAt(txn, slotIdx)
		CheckErr(err)

		// TODO: is this correct, first free overflow then release it?
		c, err := overflow.Count(txn)
		CheckErr(err)
		if c == 0 {
			pager.FreeOverflow(txn, Pointer(overflow.GetPageId()))
			h.Overflow = 0
			n.SetHeader(txn, h)
		}
	}

	err := p.DeleteAt(txn, idx)
	CheckErr(err)
}

func getAt(txn transaction.Transaction, pager *Pager2, n node, p BPage, idx int) []byte {
	if n.IsLeaf() {
		// read lock if it is a leaf node, to ensure other txn won't change it.
		if err := txn.AcquireLock(uint64(p.GetPageId()), transaction.Shared); err != nil {
			panic(err)
		}
	}

	b := p.GetAt(idx)

	if len(b) > maxPayloadSize {
		h := n.GetHeader()
		overflow, err := pager.GetOverflowReleaser(h.Overflow)
		CheckErr(err)

		slotIdx := int(DeserializePointer(b[len(b)-pointerSize:])) //todo
		restB, err := overflow.GetAt(txn, slotIdx)
		CheckErr(err)

		res := make([]byte, len(b)+len(restB)-pointerSize)
		copied := copy(res, b[:len(b)-pointerSize])
		copy(res[copied:], restB[:])

		return res
	}

	return b
}

func getKeyAt(txn transaction.Transaction, pager *Pager2, n node, p BPage, idx int) []byte {
	if n.IsLeaf() {
		// read lock if it is a leaf node, to ensure other txn won't change it.
		if err := txn.AcquireLock(uint64(p.GetPageId()), transaction.Shared); err != nil {
			panic(err)
		}
	}

	b := p.GetAt(idx)
	keySize, nn := binary.Uvarint(b)
	if keySize <= uint64(maxPayloadSize-nn) {
		return b
	} else {
		h := n.GetHeader()
		overflow, err := pager.GetOverflowReleaser(h.Overflow)
		CheckErr(err)

		slotIdx := int(DeserializePointer(b[len(b)-pointerSize:])) //todo
		restB, err := overflow.GetAt(txn, slotIdx)
		CheckErr(err)

		res := make([]byte, len(b)+len(restB)-pointerSize)
		copied := copy(res, b[:len(b)-pointerSize])
		copy(res[copied:], restB[:])

		return res
	}
}

func fitted(sp BPage) int {
	used := sp.Cap() - sp.EmptySpace()
	return (used / MaxRequiredSize) + 1
}
