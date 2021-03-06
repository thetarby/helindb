package execution

import (
	"helin/buffer"
	"helin/catalog"
	"helin/concurrency"
	"helin/concurrency/lockmanager"
)

type ExecutorContext struct {
	Txn         concurrency.Transaction
	Catalog     catalog.ICatalog
	Pool        *buffer.BufferPool
	LockManager lockmanager.ILockManager
	TxnManager  concurrency.ITxnManager
}

func NewExecutorContext(txn concurrency.Transaction, catalog catalog.ICatalog, pool *buffer.BufferPool, lckManager lockmanager.ILockManager, txnManager concurrency.ITxnManager) *ExecutorContext{
	return &ExecutorContext{
		Txn:         txn,
		Catalog:     catalog,
		Pool:        pool,
		LockManager: lckManager,
		TxnManager:  txnManager,
	}
}