package heap

import "helin/transaction"

type CopyPage interface {
	CopyAt(txn transaction.Transaction, offset uint16, data []byte) error
	GetData() []byte
	GetPageID() uint64
}

type PageReleaser interface {
	CopyPage
	Release(dirty bool)
}

type Pager interface {
	CreatePage(txn transaction.Transaction) (PageReleaser, error)
	FreePage(txn transaction.Transaction, pageID uint64) error
	GetPageToRead(txn transaction.Transaction, pageID uint64) (PageReleaser, error)
	GetPageToWrite(txn transaction.Transaction, pageID uint64) (PageReleaser, error)
}
