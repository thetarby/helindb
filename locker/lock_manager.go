package locker

import (
	"errors"
	"helin/common"
	"log"
	"sync"
	"time"
)

/*
	TODO: Check if the transaction holds a shared lock in upgrade case
*/

var ErrDeadLock = errors.New("deadlock detected")

type LockMode int

const (
	SharedLock LockMode = iota
	ExclusiveLock
)

type LockRequest struct {
	TxID     uint64
	Mode     LockMode
	Response chan error
}

type lockState struct {
	sync.RWMutex
	owners         map[uint64]LockMode
	waitQueue      []LockRequest
	waitingWriters uint
}

type LockManager struct {
	latches  common.SyncMap[uint64, *lockState]
	locks    common.SyncMap[uint64, *lockState]
	stopChan chan struct{}
}

func NewLockManager() *LockManager {
	lm := &LockManager{
		locks:    common.SyncMap[uint64, *lockState]{},
		stopChan: make(chan struct{}),
	}
	go lm.deadlockDetectorRoutine()
	return lm
}

func (lm *LockManager) TryAcquireLatch(pageID, txID uint64, mode LockMode) bool {
	ls, _ := lm.latches.LoadOrStore(pageID, &lockState{owners: make(map[uint64]LockMode)})
	ls.Lock()

	if lm.canAcquire(ls, txID, mode) {
		lm.grant(ls, txID, mode, true)
		ls.Unlock()
		return true
	}

	ls.Unlock()

	return false
}

func (lm *LockManager) AcquireLatch(pageID, txID uint64, mode LockMode) error {
	request := LockRequest{TxID: txID, Mode: mode, Response: make(chan error, 1)}

	ls, _ := lm.latches.LoadOrStore(pageID, &lockState{owners: make(map[uint64]LockMode)})
	ls.Lock()

	if lm.canAcquire(ls, txID, mode) {
		lm.grant(ls, txID, mode, true)
		ls.Unlock()
		return nil
	}

	ls.waitQueue = append(ls.waitQueue, request)
	ls.Unlock()

	// Wait for lock to be granted or transaction to be aborted
	err := <-request.Response
	if err != nil {
		return err
	}

	return nil
}

func (lm *LockManager) ReleaseLatch(pageID, txID uint64) {
	ls, exists := lm.latches.Load(pageID)
	if !exists {
		panic("unlocked non-existing lock")
	}

	ls.Lock()
	defer ls.Unlock()

	mode, ok := ls.owners[txID]
	if !ok {
		panic("unlocked non-existing lock")
	}

	delete(ls.owners, txID)

	if mode == ExclusiveLock || len(ls.owners) == 0 {
		lm.grantWaiting(ls)
	}
}

func (lm *LockManager) AcquireLock(pageID, txID uint64, mode LockMode) error {
	request := LockRequest{TxID: txID, Mode: mode, Response: make(chan error, 1)}

	ls, _ := lm.locks.LoadOrStore(pageID, &lockState{owners: make(map[uint64]LockMode)})
	ls.Lock()

	// if there is no writers waiting in the queue and lock does not conflict with any other grant it
	if ls.waitingWriters == 0 && lm.canAcquire(ls, txID, mode) {
		lm.grant(ls, txID, mode, true)
		ls.Unlock()
		return nil
	}

	// otherwise put it into the waiting queue
	ls.waitQueue = append(ls.waitQueue, request)
	if mode == ExclusiveLock {
		ls.waitingWriters++
	}
	ls.Unlock()

	// Wait for lock to be granted or transaction to be aborted
	// TODO: is it possible that txn is aborted and lock request waits forever?
	if err := <-request.Response; err != nil {
		return err
	}

	return nil
}

func (lm *LockManager) ReleaseLock(pageID, txID uint64) {
	ls, exists := lm.locks.Load(pageID)
	if !exists {
		panic("unlocked non-existing lock")
	}

	ls.Lock()
	defer ls.Unlock()

	_, ok := ls.owners[txID]
	if !ok {
		panic("unlocked non-existing lock")
	}

	delete(ls.owners, txID)

	//if mode == ExclusiveLock || len(ls.owners) == 0 {
	//	lm.grantWaiting(ls)
	//}
	lm.grantWaiting(ls)
}

func (lm *LockManager) ReleaseLocks(txID uint64) {
	lm.locks.Range(func(pageID uint64, ls *lockState) bool {
		if _, ok := ls.owners[txID]; ok {
			lm.ReleaseLock(pageID, txID)
		}

		return true
	})
}

// canAcquire returns true if lock request can be granted. It returns true if request mode is
func (lm *LockManager) canAcquire(lockInfo *lockState, txID uint64, mode LockMode) bool {
	if lockMode, ok := lockInfo.owners[txID]; ok {
		// wants the same lock it has
		if lockMode == mode {
			return true
		}

		// wants shared when has exclusive
		if mode == SharedLock {
			return true
		}

		// upgrade case, where txn already has read lock, wants the write lock and is the only owner.
		return lockMode == SharedLock && mode == ExclusiveLock && len(lockInfo.owners) == 1
	}

	if len(lockInfo.owners) == 0 {
		return true
	}

	if mode == SharedLock {
		// if there is more than one owner, it must be shared
		if len(lockInfo.owners) > 1 {
			return true
		}

		// if there is one owner it should own it in read mode
		if len(lockInfo.owners) == 1 {
			for _, lockMode := range lockInfo.owners {
				return lockMode == SharedLock
			}
		}
	}

	return false
}

// grant updates lockState so that txID added to owners.
func (lm *LockManager) grant(lockInfo *lockState, txID uint64, mode LockMode, noWait bool) {
	if mode == ExclusiveLock && !noWait {
		lockInfo.waitingWriters--
	}
	lockInfo.owners[txID] = mode
}

// grantWaiting iterates all requests in waiting queue of the lockState and grants if applicable.
func (lm *LockManager) grantWaiting(ls *lockState) {
	grantedRequests := 0

	for _, request := range ls.waitQueue {
		if lm.canAcquire(ls, request.TxID, request.Mode) {
			lm.grant(ls, request.TxID, request.Mode, false)
			request.Response <- nil
			grantedRequests++
		} else {
			break
		}
	}

	// Remove granted requests from wait queue
	ls.waitQueue = ls.waitQueue[grantedRequests:]
}

func (lm *LockManager) deadlockDetectorRoutine() {
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			wg := lm.buildWaitGraph()
			lm.detectDeadlock(wg)
		case <-lm.stopChan:
			return
		}
	}
}

func (lm *LockManager) buildWaitGraph() map[uint64]map[uint64]bool {
	graph := map[uint64]map[uint64]bool{}
	f := func(pageID uint64, ls *lockState) bool {
		ls.Lock()
		defer ls.Unlock()

		for _, request := range ls.waitQueue {
			for owner := range ls.owners {
				if _, ok := graph[request.TxID]; !ok {
					graph[request.TxID] = make(map[uint64]bool)
				}

				// can wait for itself in upgrade case
				if request.TxID != owner {
					graph[request.TxID][owner] = true
				}
			}
		}

		return true
	}

	lm.locks.Range(f)
	lm.latches.Range(f)

	return graph
}

func (lm *LockManager) detectDeadlock(waitGraph map[uint64]map[uint64]bool) {
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)

	for txID := range waitGraph {
		if lm.isCyclic(waitGraph, txID, visited, recStack) {
			minTxID := lm.findSmallestTxID(recStack)

			var txns []uint64
			for t := range recStack {
				txns = append(txns, t)
			}

			log.Printf("deadlock detected between transactions: %v, aborting: %v\n", txns, minTxID)
			lm.abortTransaction(minTxID)
			return
		}
	}
}

func (lm *LockManager) isCyclic(waitGraph map[uint64]map[uint64]bool, txID uint64, visited, recStack map[uint64]bool) bool {
	visited[txID] = true
	recStack[txID] = true

	for waitingFor := range waitGraph[txID] {
		if !visited[waitingFor] {
			if lm.isCyclic(waitGraph, waitingFor, visited, recStack) {
				return true
			}
		} else if recStack[waitingFor] {
			return true
		}
	}

	recStack[txID] = false
	return false
}

// findSmallestTxID reduces throughput, prevents starvation, increases rollback costs.
func (lm *LockManager) findSmallestTxID(transactions map[uint64]bool) uint64 {
	var minTxID uint64
	first := true
	for txID := range transactions {
		if first || txID < minTxID {
			minTxID = txID
			first = false
		}
	}
	return minTxID
}

// findLargestTxID enhances throughput, starves older transactions, reduces rollback costs.
func (lm *LockManager) findLargestTxID(transactions map[uint64]bool) uint64 {
	var maxTxID uint64
	first := true
	for txID := range transactions {
		if first || txID > maxTxID {
			maxTxID = txID
			first = false
		}
	}
	return maxTxID
}

// abortTransaction resolves all lock requests of given transaction with error
func (lm *LockManager) abortTransaction(txID uint64) {
	// TODO: optimize this. it traverses all lock requests now.
	lm.locks.Range(func(pageID uint64, ls *lockState) bool {
		for i, req := range ls.waitQueue {
			if req.TxID == txID {
				req.Response <- ErrDeadLock
				ls.waitQueue = append(ls.waitQueue[:i], ls.waitQueue[i+1:]...)
				break
			}
		}

		return true
	})
}

func (lm *LockManager) Stop() {
	close(lm.stopChan)
}
