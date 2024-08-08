package freelistv1

import (
	"helin/transaction"
)

type Pager interface {
	GetPageToRead(pageId uint64) (FreeListPage, error)
	GetPageToWrite(txn transaction.Transaction, pageId uint64, format bool) (FreeListPage, error)
}

type FreeListPage interface {
	Get() []byte
	Set(transaction.Transaction, []byte, *OpLog) error
	GetPageId() uint64
	GetLSN() uint64
	Release()
}

const (
	OpAdd    = 1
	OpPop    = 2
	OpAddSet = 3
)

type OpLog struct {
	PageID uint64
	Op     int
}
