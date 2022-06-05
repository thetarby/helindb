package btree

import (
	"bytes"
	"encoding/binary"
	"helin/buffer"
	"helin/common"
	"helin/disk/pages"
)

type PersistentPage struct {
	pages.RawPage
}

func (p PersistentPage) GetPageId() Pointer {
	return Pointer(p.RawPage.GetPageId())
}

type BufferPoolPager struct {
	pool            *buffer.BufferPool
	keySerializer   KeySerializer
	valueSerializer ValueSerializer
}

func (b *BufferPoolPager) UnpinByPointer(p Pointer, isDirty bool) {
	b.pool.Unpin(int(p), isDirty)
}

// NewInternalNode Caller should call unpin with dirty is set
func (b *BufferPoolPager) NewInternalNode(firstPointer Pointer) Node {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	p, err := b.pool.NewPage()
	common.PanicIfErr(err)
	p.WLatch()
	node := PersistentInternalNode{NodePage: &PersistentPage{RawPage: *p}, pager: b, keySerializer: b.keySerializer}

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	// write first pointer
	buf := bytes.Buffer{}
	err = binary.Write(&buf, binary.BigEndian, firstPointer)
	CheckErr(err)
	asByte := buf.Bytes()
	copy(data[PersistentNodeHeaderSize:], asByte)

	return &node
}

func (b *BufferPoolPager) NewLeafNode() Node {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	p, err := b.pool.NewPage() // TODO: handle error
	p.WLatch()
	common.PanicIfErr(err)
	node := PersistentLeafNode{NodePage: &PersistentPage{RawPage: *p}, pager: b, keySerializer: b.keySerializer, valSerializer: b.valueSerializer}

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	return &node
}

func (b *BufferPoolPager) GetNode(p Pointer, mode TraverseMode) Node {
	if p == 0 {
		return nil
	}
	page, err := b.pool.GetPage(int(p))
	common.PanicIfErr(err)
	if mode == Read{
		page.RLatch()
	}else{
		page.WLatch()
	}
	
	h := ReadPersistentNodeHeader(page.GetData())
	if h.IsLeaf == 1 {
		return &PersistentLeafNode{NodePage: &PersistentPage{RawPage: *page}, pager: b, keySerializer: b.keySerializer, valSerializer: b.valueSerializer}
	}
	return &PersistentInternalNode{NodePage: &PersistentPage{RawPage: *page}, pager: b, keySerializer: b.keySerializer}
}

func (b *BufferPoolPager) Unpin(n Node, isDirty bool) {
	b.pool.Unpin(int(n.GetPageId()), isDirty)
}

func NewBufferPoolPager(pool *buffer.BufferPool, serializer KeySerializer) *BufferPoolPager {
	return &BufferPoolPager{
		pool:            pool,
		keySerializer:   serializer,
		valueSerializer: &SlotPointerValueSerializer{},
	}
}

func NewBufferPoolPagerWithValueSize(pool *buffer.BufferPool, serializer KeySerializer, valSerializer ValueSerializer) *BufferPoolPager {
	return &BufferPoolPager{
		pool:            pool,
		keySerializer:   serializer,
		valueSerializer: valSerializer,
	}
}
