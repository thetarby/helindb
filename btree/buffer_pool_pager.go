package btree

import (
	"helin/buffer"
	"helin/common"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
)

// BtreePage is an implementation of the NodePage interface
// pages.RawPage almost implements all methods except for GetPageId()
type BtreePage struct {
	pages.RawPage
}

func (p BtreePage) GetPageId() Pointer {
	return Pointer(p.RawPage.GetPageId())
}

var _ Pager = &BufferPoolPager{}

type BufferPoolPager struct {
	pool            buffer.Pool
	keySerializer   KeySerializer
	valueSerializer ValueSerializer
	logManager      wal.LogManager
}

func (b *BufferPoolPager) Free(txn transaction.Transaction, p Pointer) error {
	txn.FreePage(uint64(p))
	return nil
}

func (b *BufferPoolPager) FreeNode(txn transaction.Transaction, n Node) {
	txn.FreePage(uint64(n.GetPageId()))
}

func (b *BufferPoolPager) CreatePage(txn transaction.Transaction) NodePage {
	p, err := b.pool.NewPage(txn)
	common.PanicIfErr(err)

	sp := InitLoggedSlottedPage(p, b.logManager)
	return &sp
}

func (b *BufferPoolPager) GetPage(p Pointer) NodePage {
	pg, err := b.pool.GetPage(uint64(p))
	common.PanicIfErr(err)

	sp := CastLoggedSlottedPage(pg, b.logManager)
	return &sp
}

func (b *BufferPoolPager) UnpinByPointer(p Pointer, isDirty bool) {
	b.pool.Unpin(uint64(p), isDirty)
}

// NewInternalNode Caller should call unpin with dirty is set
func (b *BufferPoolPager) NewInternalNode(txn transaction.Transaction, firstPointer Pointer) NodeReleaser {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	p, err := b.pool.NewPage(txn)
	common.PanicIfErr(err)
	p.WLatch()

	node := VarKeyInternalNode{
		p:             InitLoggedSlottedPage(p, b.logManager),
		keySerializer: b.keySerializer,
	}
	// set header
	node.SetHeader(txn, &h)

	// write first pointer
	node.setValueAt(txn, 0, firstPointer)

	return &writeNodeReleaser{&node, b.pool}
}

func (b *BufferPoolPager) NewLeafNode(txn transaction.Transaction) NodeReleaser {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	// TODO: if page is popped from freelist here it is modified without logging
	// but a txn release lock on freed page when it commits so no one can modify it.
	p, err := b.pool.NewPage(txn)
	common.PanicIfErr(err)
	p.WLatch()

	node := VarKeyLeafNode{
		p:             InitLoggedSlottedPage(p, b.logManager),
		keySerializer: b.keySerializer,
		valSerializer: b.valueSerializer,
	}
	// write header
	node.SetHeader(txn, &h)

	return &writeNodeReleaser{&node, b.pool}
}

func (b *BufferPoolPager) GetNode(p Pointer, mode TraverseMode) Node {
	if p == 0 {
		return nil
	}
	page, err := b.pool.GetPage(uint64(p))
	common.PanicIfErr(err)
	if mode == Read {
		page.RLatch()
	} else {
		page.WLatch()
	}

	sp := CastLoggedSlottedPage(page, b.logManager)
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

func (b *BufferPoolPager) GetNodeReleaser(p Pointer, mode TraverseMode) NodeReleaser {
	n := b.GetNode(p, mode)
	if n == nil {
		return nil
	}
	if mode == Read {
		return &readNodeReleaser{
			Node: n,
			pool: b.pool,
		}
	} else {
		return &writeNodeReleaser{
			Node: n,
			pool: b.pool,
		}
	}
}

func (b *BufferPoolPager) Unpin(n Node, isDirty bool) {
	b.pool.Unpin(uint64(n.GetPageId()), isDirty)
}

type NodeReleaser interface {
	Node
	Release()
}

type readNodeReleaser struct {
	Node
	pool buffer.Pool
}

func (n *readNodeReleaser) Release() {
	n.pool.Unpin(uint64(n.GetPageId()), false)
	n.RUnLatch()
}

type writeNodeReleaser struct {
	Node
	pool buffer.Pool
}

func (n *writeNodeReleaser) Release() {
	n.pool.Unpin(uint64(n.GetPageId()), false)
	n.WUnlatch()
}

func NewBPP(pool buffer.Pool, serializer KeySerializer, valSerializer ValueSerializer, logManager wal.LogManager) *BufferPoolPager {
	return &BufferPoolPager{
		pool:            pool,
		keySerializer:   serializer,
		valueSerializer: valSerializer,
		logManager:      logManager,
	}
}
