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

func (B *BPFreeList) AddInRecovery(txn transaction.Transaction, pageId uint64, undoNext pages.LSN) error {
	//TODO implement me
	panic("implement me")
}

func (B *BPFreeList) GetHeaderPageLsn() pages.LSN {
	return pages.LSN(B.List.GetHeaderPageLsn())
}

func NewBPFreeList(pool Pool, lm wal.LogManager, init bool) *BPFreeList {
	return &BPFreeList{List: freelistv1.NewFreeList(NewBufferPoolPager(pool, lm), init)}
}
