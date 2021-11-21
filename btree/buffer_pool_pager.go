package btree

import (
	"bytes"
	"encoding/binary"
	"helin/buffer"
	"helin/disk/pages"
)

type RealPersistentPage struct {
	pages.RawPage
}

func (p RealPersistentPage) GetPageId() Pointer {
	return Pointer(p.RawPage.GetPageId())
}

type BufferPoolPager struct {
	pool *buffer.BufferPool
}

func (b *BufferPoolPager) NewInternalNode(firstPointer Pointer) Node {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	p,_ := b.pool.NewPage()
	node := PersistentInternalNode{PersistentPage: &RealPersistentPage{RawPage:*p}, pager: b}

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

func (b *BufferPoolPager) NewLeafNode() Node {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	p,_ := b.pool.NewPage()
	node := PersistentLeafNode{PersistentPage: &RealPersistentPage{RawPage:*p}, pager: b}

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	return &node
}

func (b *BufferPoolPager) GetNode(p Pointer) Node {
	if p == 0 {
		return nil
	}
	page,_ := b.pool.GetPage(int(p))
	h := ReadPersistentNodeHeader(page.GetData())
	if h.IsLeaf == 1{
		return &PersistentLeafNode{PersistentPage: &RealPersistentPage{RawPage:*page}, pager: b}
	}
	return &PersistentInternalNode{PersistentPage: &RealPersistentPage{RawPage:*page}, pager: b}
}

func (b *BufferPoolPager) Unpin(n Node, isDirty bool) {
	b.pool.Unpin(int(n.GetPageId()), isDirty)
}

func NewBufferPoolPager(pool *buffer.BufferPool) *BufferPoolPager {
	return &BufferPoolPager{
		pool: pool,
	}
}

