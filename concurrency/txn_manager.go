package concurrency

import (
	"helin/buffer"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
	"sync"
	"sync/atomic"
	"time"
)

var _ transaction.Transaction = &txn{}

type txn struct {
	id         transaction.TxnID
	freedPages []uint64
	prevLsn    pages.LSN
	undoing    []byte
}

func newTxn(id transaction.TxnID, freedPages []uint64, prevLsn pages.LSN) *txn {
	return &txn{id: id, freedPages: freedPages, prevLsn: prevLsn}
}

func (t *txn) SetUndoingLog(bytes []byte) {
	t.undoing = bytes
}

func (t *txn) SetPrevLsn(lsn pages.LSN) {
	t.prevLsn = lsn
}

func (t *txn) setFreedPages(freedPages []uint64) {
	t.freedPages = freedPages
}

func (t *txn) GetPrevLsn() pages.LSN {
	return t.prevLsn
}

func (t *txn) GetID() transaction.TxnID {
	return t.id
}

func (t *txn) FreePage(pageID uint64) {
	t.freedPages = append(t.freedPages, pageID)
}

func (t *txn) GetUndoingLog() []byte {
	return t.undoing
}

// TxnManager keeps track of running transactions.
type TxnManager interface {
	Begin() transaction.Transaction
	Commit(transaction.Transaction) error
	AsyncCommit(transaction transaction.Transaction) error
	CommitByID(transaction.TxnID) error
	Abort(transaction.Transaction)
	AbortByID(id transaction.TxnID)

	BlockAllTransactions()
	ResumeTransactions()

	BlockNewTransactions()
	ResumeNewTransactions()

	ActiveTransactions() []transaction.TxnID
}

var _ TxnManager = &TxnManagerImpl{}

type TxnManagerImpl struct {
	actives    map[transaction.TxnID]*txn
	lm         wal.LogManager
	r          *Recovery
	txnCounter atomic.Int64
	mut        *sync.Mutex
	newTxn     *sync.RWMutex
	pool       buffer.Pool
}

func NewTxnManager(pool buffer.Pool, lm wal.LogManager) *TxnManagerImpl {
	return &TxnManagerImpl{
		actives:    map[transaction.TxnID]*txn{},
		lm:         lm,
		r:          nil,
		txnCounter: atomic.Int64{},
		mut:        &sync.Mutex{},
		newTxn:     &sync.RWMutex{},
		pool:       pool,
	}
}

func (t *TxnManagerImpl) Begin() transaction.Transaction {
	t.newTxn.RLock()
	defer t.newTxn.RUnlock()

	t.mut.Lock()
	defer t.mut.Unlock()

	// lsn is used as txnID
	lsn := t.lm.AppendLog(nil, wal.NewTxnStarterLogRecord())
	txn := newTxn(transaction.TxnID(lsn), nil, 0)
	t.actives[txn.GetID()] = txn

	return txn
}

var s = time.Now()

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
	t.AbortByID(transaction.GetID())
}

func (t *TxnManagerImpl) CommitByID(id transaction.TxnID) error {
	t.mut.Lock()
	txn := t.actives[id]
	t.mut.Unlock()

	_, err := t.lm.WaitAppendLog(txn, wal.NewCommitLogRecord(id, txn.freedPages))
	if err != nil {
		return err
	}

	// IMPORTANT NOTE: if a checkpoint begins right at this line commit log record is persisted but active txn table
	// still includes this log record. Hence, in undo phase there might seem commit log records. In that case that
	// txn should not be rolled back.
	t.mut.Lock()
	defer t.mut.Unlock()

	for _, page := range txn.freedPages {
		if err := t.pool.FreePage(txn, page, true); err != nil {
			// TODO: handle this
			panic(err)
		}
	}

	t.lm.AppendLog(txn, wal.NewTxnEndLogRecord(id))
	delete(t.actives, id)

	return nil
}

func (t *TxnManagerImpl) AbortByID(id transaction.TxnID) {
	// 1. create an iterator on logs that will iterate a transaction's logs in reverse order
	// 2. create clr logs that are basically logical negations of corresponding logs
	// 3. apply clr records and append them to wal
	// 4. append abort log

	// create a log iterator starting from given lsn
	//lsn := t.lm.WaitAppendLog(wal.NewAbortLogRecord(id))
	//wal.NewTxnBackwardLogIterator(id)

	// TODO: implement this
	//logs := wal.NewTxnBackwardLogIterator(id, nil)
	//for {
	//	lr, err := logs.Prev()
	//	if err != nil {
	//		// TODO: what to do?
	//		panic(err)
	//	}
	//
	//	if lr == nil {
	//		// if logs are finished it is rolled back
	//		break
	//	}
	//
	//	if err := t.r.Undo(lr, 0); err != nil {
	//		panic(err)
	//	}
	//}
	panic("implement me")
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
