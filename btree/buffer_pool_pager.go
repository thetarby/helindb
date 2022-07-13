package btree

import (
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
	//node := PersistentInternalNode{NodePage: &PersistentPage{RawPage: *p}, pager: b, keySerializer: b.keySerializer}
	nnode := VarKeyInternalNode{
		p:             InitSlottedPage(*p),
		keySerializer: b.keySerializer,
	}
	// set header
 	nnode.SetHeader(&h)

	// write first pointer
	nnode.setValueAt(0, firstPointer)

	return &nnode
}

func (b *BufferPoolPager) NewLeafNode() Node {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	p, err := b.pool.NewPage() // TODO: handle error
	common.PanicIfErr(err)
	p.WLatch()
	//node := PersistentLeafNode{NodePage: &PersistentPage{RawPage: *p}, pager: b, keySerializer: b.keySerializer, valSerializer: b.valueSerializer}
	nnode := VarKeyLeafNode{
		p:             InitSlottedPage(*p),
		keySerializer: b.keySerializer,
		valSerializer: b.valueSerializer,
	}
	// write header
	nnode.SetHeader(&h)

	return &nnode
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
 	sp := CastSlottedPage(*page)
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
