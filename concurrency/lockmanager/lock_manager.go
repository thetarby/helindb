package lockmanager

import (
	"helin/concurrency"
	"helin/disk/structures"
)

type ILockManager interface{
	LockShared(txn concurrency.Transaction, rid *structures.Rid) error
	LockExclusive(txn concurrency.Transaction, rid *structures.Rid) error
	LockUpgrade(txn concurrency.Transaction, rid *structures.Rid) error
	Unlock(txn concurrency.Transaction, rid *structures.Rid) error
}