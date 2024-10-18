package concurrency

import (
	"helin/buffer"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/locker"
	"helin/transaction"
	"sync"
)

var _ transaction.Transaction = &txn{}

type txn struct {
	mut        *sync.RWMutex
	id         transaction.TxnID
	freedPages []uint64
	prevLsn    pages.LSN
	undoing    []byte
	locker     *locker.LockManager
	locks      map[uint64]transaction.LockType
}

func newTxn(id transaction.TxnID, freedPages []uint64, prevLsn pages.LSN, locker *locker.LockManager) *txn {
	return &txn{
		mut:        &sync.RWMutex{},
		id:         id,
		freedPages: freedPages,
		prevLsn:    prevLsn,
		undoing:    nil,
		locker:     locker,
		locks:      map[uint64]transaction.LockType{},
	}
}

func (t *txn) AcquireLatch(pageID uint64, lockType transaction.LockType) error {
	if err := t.locker.AcquireLatch(pageID, uint64(t.id), locker.LockMode(lockType)); err != nil {
		return err
	}

	return nil
}

func (t *txn) ReleaseLatch(pageID uint64) {
	t.locker.ReleaseLatch(pageID, uint64(t.id))
}

func (t *txn) AcquireLock(pageID uint64, lockType transaction.LockType) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	// if it already has a more exclusive lock type then return directly
	if existingLockType, ok := t.locks[pageID]; ok {
		if existingLockType >= lockType {
			return nil
		}
	}

	if err := t.locker.AcquireLock(pageID, uint64(t.id), locker.LockMode(lockType)); err != nil {
		return err
	}

	t.locks[pageID] = lockType
	return nil
}

func (t *txn) SetUndoingLog(bytes []byte) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.undoing = bytes
}

func (t *txn) SetPrevLsn(lsn pages.LSN) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.prevLsn = lsn
}

func (t *txn) setFreedPages(freedPages []uint64) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.freedPages = freedPages
}

func (t *txn) GetPrevLsn() pages.LSN {
	t.mut.RLock()
	defer t.mut.RUnlock()

	return t.prevLsn
}

func (t *txn) GetID() transaction.TxnID {
	return t.id
}

func (t *txn) FreePage(pageID uint64) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.freedPages = append(t.freedPages, pageID)
}

func (t *txn) GetUndoingLog() []byte {
	return t.undoing
}

func (t *txn) ReleaseLocks() {
	for l := range t.locks {
		t.locker.ReleaseLock(l, uint64(t.id))
	}
}

// TxnManager keeps track of running transactions.
type TxnManager interface {
	Begin() transaction.Transaction
	Commit(transaction.Transaction) error
	AsyncCommit(transaction transaction.Transaction) error
	GetByID(transaction.TxnID) transaction.Transaction
	CommitByID(transaction.TxnID) error
	Abort(transaction.Transaction)
	AbortByID(id transaction.TxnID)

	BlockAllTransactions()
	ResumeTransactions()

	BlockNewTransactions()
	ResumeNewTransactions()

	ActiveTransactions() []transaction.TxnID

	// Close is a blocking operation that stops new transactions and waits until all active transactions are finished.
	Close()
}

var _ TxnManager = &TxnManagerImpl{}

type TxnManagerImpl struct {
	actives     map[transaction.TxnID]*txn
	noActives   *sync.Cond
	lm          wal.LogManager
	mut         *sync.Mutex
	newTxn      *sync.RWMutex
	pool        buffer.Pool
	locker      *locker.LockManager
	segmentSize uint64
	dir         string
}

func NewTxnManager(pool buffer.Pool, lm wal.LogManager, locker *locker.LockManager, segmentSize uint64, dir string) *TxnManagerImpl {
	mut := &sync.Mutex{}
	return &TxnManagerImpl{
		actives:     map[transaction.TxnID]*txn{},
		noActives:   sync.NewCond(mut),
		lm:          lm,
		mut:         mut,
		newTxn:      &sync.RWMutex{},
		pool:        pool,
		locker:      locker,
		segmentSize: segmentSize,
		dir:         dir,
	}
}

func (t *TxnManagerImpl) GetByID(id transaction.TxnID) transaction.Transaction {
	return t.actives[id]
}

func (t *TxnManagerImpl) Begin() transaction.Transaction {
	t.newTxn.RLock()
	defer t.newTxn.RUnlock()

	t.mut.Lock()
	defer t.mut.Unlock()

	// lsn is used as txnID
	lsn := t.lm.AppendLog(nil, wal.NewTxnStarterLogRecord())
	txn := newTxn(transaction.TxnID(lsn), nil, 0, t.locker)
	t.actives[txn.GetID()] = txn

	return txn
}

// Commit waits until commit record is flushed. Hence, it guarantees that txn is committed to persistent storage.
func (t *TxnManagerImpl) Commit(transaction transaction.Transaction) error {
	if err := t.CommitByID(transaction.GetID()); err != nil {
		return err
	}

	return nil
}

// AsyncCommit does not wait for commit record to be flushed.
func (t *TxnManagerImpl) AsyncCommit(transaction transaction.Transaction) error {
	t.mut.Lock()
	defer t.mut.Unlock()

	txn := t.actives[transaction.GetID()]
	t.lm.AppendLog(transaction, wal.NewCommitLogRecord(transaction.GetID(), txn.freedPages))
	delete(t.actives, transaction.GetID())
	return nil
}

func (t *TxnManagerImpl) Abort(transaction transaction.Transaction) {
	// TODO important: use a pool for read iterators and close them when no longer needed
	if t.lm.GetFlushedLSNOrZero() < transaction.GetPrevLsn() {
		if err := t.lm.Flush(); err != nil {
			panic(err)
		}
	}

	iter, err := wal.OpenBwalLogIter(t.segmentSize, t.dir, wal.NewDefaultSerDe())
	if err != nil {
		panic(err)
	}

	r := NewRecovery(iter, t.lm, nil, t.pool)

	r.RollbackTxn(transaction)

	transaction.ReleaseLocks()
}

func (t *TxnManagerImpl) CommitByID(id transaction.TxnID) error {
	t.mut.Lock()
	txn := t.actives[id]
	t.mut.Unlock()

	lsn := t.lm.AppendLog(txn, wal.NewCommitLogRecord(id, txn.freedPages))

	for lock := range txn.locks {
		txn.locker.ReleaseLock(lock, uint64(id))
	}

	if err := t.lm.Wait(lsn); err != nil {
		return err
	}

	// IMPORTANT NOTE: if a checkpoint begins right at this line commit log record is persisted but active txn table
	// still includes this log record. Hence, in undo phase there might seem commit log records. In that case that
	// txn should not be rolled back.
	t.mut.Lock()
	defer t.mut.Unlock()

	for _, page := range txn.freedPages {
		if err := t.pool.FreePage(txn, page); err != nil {
			// TODO: handle this
			panic(err)
		}
	}

	t.lm.AppendLog(txn, wal.NewTxnEndLogRecord(id))
	delete(t.actives, id)

	if len(t.actives) == 0 {
		t.noActives.Broadcast()
	}

	return nil
}

func (t *TxnManagerImpl) AbortByID(id transaction.TxnID) {
	t.Abort(t.actives[id])
}

func (t *TxnManagerImpl) BlockAllTransactions() {
	t.mut.Lock()
}

func (t *TxnManagerImpl) ResumeTransactions() {
	t.mut.Unlock()
}

func (t *TxnManagerImpl) BlockNewTransactions() {
	t.newTxn.Lock()
}

func (t *TxnManagerImpl) ResumeNewTransactions() {
	t.newTxn.Unlock()
}

func (t *TxnManagerImpl) ActiveTransactions() []transaction.TxnID {
	res := make([]transaction.TxnID, 0, len(t.actives))
	for id := range t.actives {
		res = append(res, id)
	}
	return res
}

func (t *TxnManagerImpl) Close() {
	t.BlockNewTransactions()

	t.mut.Lock()
	for len(t.actives) > 0 {
		t.noActives.Wait()
	}

	t.mut.Unlock()
}
