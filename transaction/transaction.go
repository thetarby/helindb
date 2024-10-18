package transaction

import (
	"helin/disk/pages"
	"sync"
	"sync/atomic"
)

// LockType represents the type of lock
type LockType int

const (
	Shared LockType = iota
	Exclusive
)

type Transaction interface {
	GetID() TxnID
	FreePage(pageID uint64)
	SetPrevLsn(pages.LSN)
	GetPrevLsn() pages.LSN
	GetUndoingLog() []byte
	SetUndoingLog([]byte)

	AcquireLock(pageID uint64, lockType LockType) error
	AcquireLatch(pageID uint64, lockType LockType) error
	ReleaseLatch(pageID uint64)

	ReleaseLocks()
}

func TxnTODO() Transaction {
	return TxnNoop()
}

type TxnID uint64

var noOpTxnCounter uint64 = 0

func TxnNoop() Transaction {
	id := atomic.AddUint64(&noOpTxnCounter, 1)
	return txnNoop{
		id: TxnID(id),
	}
}

var mut sync.RWMutex
var locks map[uint64]*struct {
	m      *sync.RWMutex
	shared bool
}

// TODO Important: since this is package level parallel tests do not work. change it.
func init() {
	locks = make(map[uint64]*struct {
		m      *sync.RWMutex
		shared bool
	})
}

var _ Transaction = &txnNoop{}

type txnNoop struct {
	id TxnID
}

func (t txnNoop) ReleaseLocks() {
	return
}

func (t txnNoop) AcquireLatch(pageID uint64, lockType LockType) error {
	mut.Lock()

	l, ok := locks[pageID]
	if !ok {
		locks[pageID] = &struct {
			m      *sync.RWMutex
			shared bool
		}{m: &sync.RWMutex{}, shared: false}
		l = locks[pageID]
	}

	mut.Unlock()

	if lockType == Shared {
		l.m.RLock()
		l.shared = true
	} else {
		l.m.Lock()
		l.shared = false
	}

	return nil
}

func (t txnNoop) ReleaseLatch(pageID uint64) {
	mut.Lock()
	defer mut.Unlock()

	l := locks[pageID]
	if l.shared {
		l.m.RUnlock()
	} else {
		l.m.Unlock()
	}
}

func (t txnNoop) AcquireLock(pageID uint64, lockType LockType) error {
	return nil
}

func (t txnNoop) GetUndoingLog() []byte {
	return nil
}

func (t txnNoop) SetUndoingLog([]byte) {
	return
}

func (t txnNoop) SetPrevLsn(lsn pages.LSN) {
	return
}

func (t txnNoop) GetPrevLsn() pages.LSN {
	return pages.ZeroLSN
}

func (t txnNoop) FreePage(pageID uint64) {
	return
}

func (t txnNoop) GetID() TxnID {
	return t.id
}
