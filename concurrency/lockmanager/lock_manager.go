package lockmanager

import (
	"helin/disk/structures"
	"helin/transaction"
	"sync"
	"sync/atomic"
)

type ILockManager interface {
	LockShared(txn transaction.Transaction, rid *structures.Rid) error
	LockExclusive(txn transaction.Transaction, rid *structures.Rid) error
	LockUpgrade(txn transaction.Transaction, rid *structures.Rid) error
	Unlock(txn transaction.Transaction, rid *structures.Rid) error
}

type LockManager struct {
	globalMut *sync.RWMutex
	nextTxnID atomic.Int64
}

func (l *LockManager) getNextTxnID() int64 {
	return l.nextTxnID.Add(1)
}

func (l *LockManager) LockAll() {
	l.globalMut.Lock()
}

func (l *LockManager) ResumeAll() {
	l.globalMut.Unlock()
}
