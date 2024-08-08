package pbtree

import (
	"helin/btree/btree"
	"helin/buffer"
	"helin/common"
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

func (b *BufferPoolBPager) NewBPage(txn transaction.Transaction) btree.BPage {
	p, err := b.pool.NewPage(txn)
	common.PanicIfErr(err)
	p.WLatch()

	return btree.InitLoggedSlottedPage(txn, p, b.lm)
}

func (b *BufferPoolBPager) GetBPage(pointer btree.Pointer) (btree.BPage, error) {
	p, err := b.pool.GetPage(uint64(pointer))
	if err != nil {
		return nil, err
	}

	// NOTE: casting asserts  by reading pages header hence the read latch.
	p.RLatch()
	lsp := btree.CastLoggedSlottedPage(p, b.lm)
	p.RUnLatch()

	return &lsp, nil
}

func (b *BufferPoolBPager) Unpin(p btree.Pointer) {
	b.pool.Unpin(uint64(p), true)
}

func (b *BufferPoolBPager) FreeBPage(txn transaction.Transaction, p btree.Pointer) {
	txn.FreePage(uint64(p))
}

func (b *BufferPoolBPager) CreateOverflow(txn transaction.Transaction) btree.OverflowReleaser {
	slotSize := disk.PageUsableSize / btree.MaxRequiredSize

	h, err := heap.InitHeap(txn, slotSize, MaxPageSizeForOverflow, uint16(disk.PageUsableSize), b.heapPager)
	common.PanicIfErr(err)

	return h
}

func (b *BufferPoolBPager) FreeOverflow(txn transaction.Transaction, p btree.Pointer) {
	h, err := heap.OpenHeap(uint64(p), MaxPageSizeForOverflow, uint16(disk.PageUsableSize), b.heapPager)
	common.PanicIfErr(err)

	err = h.Free(txn)
	common.PanicIfErr(err)
}

func (b *BufferPoolBPager) GetOverflowReleaser(p btree.Pointer) btree.OverflowReleaser {
	h, err := heap.OpenHeap(uint64(p), MaxPageSizeForOverflow, uint16(disk.PageUsableSize), b.heapPager)
	common.PanicIfErr(err)

	return h
}
