package btree

import (
	"bytes"
	"encoding/binary"
)

/* InternalNode and SlottedPage structures should extend a PersistentPage implementation to be able to be disk persistent */

type PersistentPage interface {
	GetData() []byte

	// GetPageId returns the page_id of the physical page.
	GetPageId() Pointer
}

type Pager interface {
	// NewInternalNode first should create a PersistentPage which points to a byte array.
	// Then initialize an InternalNode structure.
	// Finally, it should serialize the structure on to pointed byte array.
	// NOTE: the node should have a reference(by extending it for example) to the created PersistentPage
	// so that it can be serialized in the future when its state changes.
	NewInternalNode(firstPointer Pointer) Node

	// NewLeafNode first should create an PersistentPage which points to a byte array.
	// Then initialize an LeafNode structure.
	// Finally, it should serialize the structure on to pointed byte array
	NewLeafNode() Node

	// GetNode returns a Node given a Pointer. Should be able to deserialize a node from byte arr and should be able to
	// recognize if it is an InternalNode or LeafNode and return the correct type.
	GetNode(p Pointer) Node

	Unpin(n Node, isDirty bool)

	UnpinByPointer(p Pointer, isDirty bool)
}

/* NOOP IMPLEMENTATION*/

type NoopPersistentPage struct {
	pageId Pointer
	data   []byte
}

func NewNoopPersistentPage(pageId Pointer) *NoopPersistentPage {
	return &NoopPersistentPage{
		pageId: pageId,
		data:   make([]byte, 4096, 4096),
	}
}

func (n NoopPersistentPage) GetData() []byte {
	return n.data
}

func (n NoopPersistentPage) GetPageId() Pointer {
	return n.pageId
}

type NoopPager struct {
}

func (b *NoopPager) UnpinByPointer(p Pointer, isDirty bool) {}

func (b *NoopPager) Unpin(n Node, isDirty bool) {}

var lastPageId Pointer = 0

func (b *NoopPager) NewInternalNode(firstPointer Pointer) Node {
	// TODO: create persistent page from buffer pool
	lastPageId++
	i := InternalNode{
		PersistentPage: NewNoopPersistentPage(lastPageId),
		Keys:           make([]Key, 0, 2),
		Pointers:       []Pointer{firstPointer},
		pager:          b,
	}
	mapping[lastPageId] = &i

	return &i
}

func (b *NoopPager) NewLeafNode() Node {
	// TODO: create persistent page from buffer pool
	lastPageId++
	l := LeafNode{
		PersistentPage: NewNoopPersistentPage(lastPageId),
		Keys:           make([]Key, 0, 2),
		Values:         make([]interface{}, 0, 2),
		pager:          b,
	}
	mapping[lastPageId] = &l

	return &l
}

var mapping = make(map[Pointer]Node)

func (b *NoopPager) GetNode(p Pointer) Node {
	return mapping[p]
}

type NoopPersistentPager struct {
	KeySerializer KeySerializer
	KeySize       int
}

func (n2 NoopPersistentPager) UnpinByPointer(p Pointer, isDirty bool) {
	panic("implement me")
}

func (n2 NoopPersistentPager) Unpin(n Node, isDirty bool) {

}

func (n NoopPersistentPager) NewInternalNode(firstPointer Pointer) Node {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	// create a new node
	// TODO: should use an adam akıllı pager
	lastPageId++
	node := PersistentInternalNode{PersistentPage: NewNoopPersistentPage(lastPageId), pager: n, serializer: n.KeySerializer, keySize: n.KeySize}

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	// write first pointer
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, firstPointer)
	CheckErr(err)
	asByte := buf.Bytes()
	copy(data[PersistentNodeHeaderSize:], asByte)

	mapping[lastPageId] = &node
	return &node
}

func (n NoopPersistentPager) NewLeafNode() Node {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	// create a new node
	// TODO: should use an adam akıllı pager
	lastPageId++
	node := PersistentLeafNode{PersistentPage: NewNoopPersistentPage(lastPageId), pager: n, serializer: n.KeySerializer, keySize: n.KeySize}

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	mapping[lastPageId] = &node
	return &node
}

var mapping2 = make(map[Pointer]Node)

func (n NoopPersistentPager) GetNode(p Pointer) Node {
	return mapping[p]
}
