package pheap

import (
	"helin/common"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/heap/heap"
	"helin/transaction"
)

type LoggedHeapPage struct {
	p  *pages.CopyAtPage
	lm wal.LogManager
}

var _ heap.Pager = &BufferPoolPager{}

func (h *LoggedHeapPage) CopyAt(txn transaction.Transaction, offset uint16, data []byte) error {
	lr := wal.NewCopyAtLogRecord(txn.GetID(), offset, common.Clone(data), common.Clone(h.p.ReadAt(offset, len(data))), h.GetPageID())
	lsn := h.lm.AppendLog(txn, lr)

	h.p.CopyAt(offset, data)
	h.p.SetPageLSN(lsn)
	h.p.SetDirty()

	return nil
}

func (h *LoggedHeapPage) GetData() []byte {
	return h.p.GetData()
}

func (h *LoggedHeapPage) GetPageID() uint64 {
	return h.p.GetPageId()
}
