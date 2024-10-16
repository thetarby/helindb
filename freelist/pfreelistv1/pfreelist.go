package pfreelistv1

import (
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/freelist/freelistv1"
	"helin/transaction"
)

type BPFreeList struct {
	*freelistv1.List
}

func (B *BPFreeList) GetHeaderPageLsn(txn transaction.Transaction) pages.LSN {
	return pages.LSN(B.List.GetHeaderPageLsn(txn))
}

func NewBPFreeList(pool Pool, lm wal.LogManager, init bool) *BPFreeList {
	return &BPFreeList{List: freelistv1.NewFreeList(NewBufferPoolPager(pool, lm), init)}
}
