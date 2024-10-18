package pbtree

import (
	"helin/btree/btree"
	"helin/buffer"
	"helin/disk"
	"helin/disk/wal"
	"helin/heap/heap"
	"helin/heap/pheap"
	"helin/transaction"
)

const (
	MaxPageSizeForOverflow = 8
)

var _ btree.BPager = &BufferPoolBPager{}

// BufferPoolBPager is a btree.BPager implementation using buffer pool so that it is persistent.
type BufferPoolBPager struct {
	pool      buffer.Pool
	lm        wal.LogManager
	heapPager *pheap.BufferPoolPager
}

func NewBufferPoolBPager(pool buffer.Pool, lm wal.LogManager) *BufferPoolBPager {
	return &BufferPoolBPager{pool: pool, lm: lm, heapPager: pheap.NewBufferPoolPager(pool, lm)}
}

func (b *BufferPoolBPager) NewBPage(txn transaction.Transaction) (btree.BPageReleaser, error) {
	p, err := b.pool.NewPage(txn)
	if err != nil {
		return nil, err
	}

	//p.WLatch()
	if err := txn.AcquireLatch(p.GetPageId(), transaction.Exclusive); err != nil {
		return nil, err
	}

	lsp := btree.InitLoggedSlottedPage(txn, p, b.lm)

	return &writeBpageReleaser{
		LoggedSlottedPage: lsp,
		bpager:            b,
		txn:               txn,
	}, nil
}

func (b *BufferPoolBPager) GetBPageToRead(txn transaction.Transaction, pointer btree.Pointer) (btree.BPageReleaser, error) {
	p, err := b.pool.GetPage(uint64(pointer))
	if err != nil {
		return nil, err
	}

	//p.RLatch()
	if err := txn.AcquireLatch(p.GetPageId(), transaction.Shared); err != nil {
		return nil, err
	}

	lsp := btree.CastLoggedSlottedPage(p, b.lm)

	return &readBpageReleaser{
		LoggedSlottedPage: &lsp,
		bpager:            b,
		txn:               txn,
	}, nil
}

func (b *BufferPoolBPager) GetBPageToWrite(txn transaction.Transaction, pointer btree.Pointer) (btree.BPageReleaser, error) {
	p, err := b.pool.GetPage(uint64(pointer))
	if err != nil {
		return nil, err
	}

	//p.WLatch()
	if err := txn.AcquireLatch(p.GetPageId(), transaction.Exclusive); err != nil {
		return nil, err
	}

	lsp := btree.CastLoggedSlottedPage(p, b.lm)

	return &writeBpageReleaser{
		LoggedSlottedPage: &lsp,
		bpager:            b,
		txn:               txn,
	}, nil
}

func (b *BufferPoolBPager) Unpin(p btree.Pointer) {
	b.pool.Unpin(uint64(p), true)
}

func (b *BufferPoolBPager) FreeBPage(txn transaction.Transaction, p btree.Pointer) {
	txn.FreePage(uint64(p))
}

func (b *BufferPoolBPager) CreateOverflow(txn transaction.Transaction) (btree.OverflowReleaser, error) {
	slotSize := disk.PageUsableSize / btree.MaxRequiredSize

	h, err := heap.InitHeap(txn, slotSize, MaxPageSizeForOverflow, uint16(disk.PageUsableSize), b.heapPager)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func (b *BufferPoolBPager) FreeOverflow(txn transaction.Transaction, p btree.Pointer) error {
	h, err := heap.OpenHeap(uint64(p), MaxPageSizeForOverflow, uint16(disk.PageUsableSize), b.heapPager)
	if err != nil {
		return err
	}

	if err := h.Free(txn); err != nil {
		return err
	}

	return nil
}

func (b *BufferPoolBPager) GetOverflowReleaser(p btree.Pointer) (btree.OverflowReleaser, error) {
	h, err := heap.OpenHeap(uint64(p), MaxPageSizeForOverflow, uint16(disk.PageUsableSize), b.heapPager)
	if err != nil {
		return nil, err
	}

	return h, nil
}

type readBpageReleaser struct {
	*btree.LoggedSlottedPage
	bpager btree.BPager
	txn    transaction.Transaction
}

func (n *readBpageReleaser) Release() {
	n.bpager.Unpin(n.GetPageId())
	// n.RUnLatch()
	n.txn.ReleaseLatch(uint64(n.GetPageId()))
}

type writeBpageReleaser struct {
	*btree.LoggedSlottedPage
	bpager btree.BPager
	txn    transaction.Transaction
}

func (n *writeBpageReleaser) Release() {
	n.bpager.Unpin(n.GetPageId())
	//n.WUnlatch()
	n.txn.ReleaseLatch(uint64(n.GetPageId()))
}
