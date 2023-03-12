package execution

import (
	"helin/buffer"
	"helin/catalog"
	"helin/concurrency"
	"helin/transaction"
)

type ExecutorContext struct {
	Txn        transaction.Transaction
	Catalog    catalog.Catalog
	Pool       *buffer.BufferPool
	TxnManager concurrency.TxnManager
}

func NewExecutorContext(txn transaction.Transaction, catalog catalog.Catalog, pool *buffer.BufferPool, txnManager concurrency.TxnManager) *ExecutorContext {
	return &ExecutorContext{
		Txn:        txn,
		Catalog:    catalog,
		Pool:       pool,
		TxnManager: txnManager,
	}
}
