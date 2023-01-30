package execution

import (
	"helin/buffer"
	"helin/catalog"
	"helin/concurrency"
	"helin/concurrency/lockmanager"
	"helin/transaction"
)

type ExecutorContext struct {
	Txn         transaction.Transaction
	Catalog     catalog.Catalog
	Pool        *buffer.BufferPool
	LockManager lockmanager.ILockManager
	TxnManager  concurrency.TxnManager
}

func NewExecutorContext(txn transaction.Transaction, catalog catalog.Catalog, pool *buffer.BufferPool, lckManager lockmanager.ILockManager, txnManager concurrency.TxnManager) *ExecutorContext {
	return &ExecutorContext{
		Txn:         txn,
		Catalog:     catalog,
		Pool:        pool,
		LockManager: lckManager,
		TxnManager:  txnManager,
	}
}
