package btree

import (
	"helin/buffer"
	"helin/common"
	"helin/disk/pages"
)

// BtreePage is an implementation of the NodePage interface
// pages.RawPage almost implements all methods except for GetPageId() 
type BtreePage struct {
	pages.RawPage
}

func (p BtreePage) GetPageId() Pointer {
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
	
	node := VarKeyInternalNode{
		p:             InitSlottedPage(&BtreePage{*p}),
		keySerializer: b.keySerializer,
	}
	// set header
 	node.SetHeader(&h)

	// write first pointer
	node.setValueAt(0, firstPointer)

	return &node
}

func (b *BufferPoolPager) NewLeafNode() Node {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	p, err := b.pool.NewPage() // TODO: handle error
	common.PanicIfErr(err)
	p.WLatch()

	node := VarKeyLeafNode{
		p:             InitSlottedPage(&BtreePage{*p}),
		keySerializer: b.keySerializer,
		valSerializer: b.valueSerializer,
	}
	// write header
	node.SetHeader(&h)

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
 	sp := CastSlottedPage(&BtreePage{*page})
	h := ReadPersistentNodeHeader(sp.GetAt(0))
	if h.IsLeaf == 1 {
		return &VarKeyLeafNode{
			p:             sp,
			keySerializer: b.keySerializer,
			valSerializer: b.valueSerializer,
		}
	}
	return &VarKeyInternalNode{
		p:             sp,
		keySerializer: b.keySerializer,
	}
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

func NewBufferPoolPagerWithValueSerializer(pool *buffer.BufferPool, serializer KeySerializer, valSerializer ValueSerializer) *BufferPoolPager {
	return &BufferPoolPager{
		pool:            pool,
		keySerializer:   serializer,
		valueSerializer: valSerializer,
	}
}
