package concurrency

import (
	"helin/buffer"
	"helin/disk/wal"
	"helin/transaction"
	"sync"
	"sync/atomic"
)

type txn struct {
	id         transaction.TxnID
	freedPages []uint64
}

func (t *txn) GetID() transaction.TxnID {
	return t.id
}

func (t *txn) FreePage(pageID uint64) {
	t.freedPages = append(t.freedPages, pageID)
}

// TxnManager keeps track of running transactions.
type TxnManager interface {
	Begin() transaction.Transaction
	Commit(transaction.Transaction)
	AsyncCommit(transaction transaction.Transaction)
	CommitByID(transaction.TxnID)
	Abort(transaction.Transaction)
	AbortByID(id transaction.TxnID)

	BlockAllTransactions()
	ResumeTransactions()

	ActiveTransactions() []transaction.TxnID
}

var _ TxnManager = &TxnManagerImpl{}

type TxnManagerImpl struct {
	actives    map[transaction.TxnID]*txn
	lm         wal.LogManager
	r          *Recovery
	txnCounter atomic.Int64
	mut        *sync.Mutex
	pool       buffer.IBufferPool
}

func NewTxnManager(pool buffer.IBufferPool, lm wal.LogManager) *TxnManagerImpl {
	return &TxnManagerImpl{
		actives:    map[transaction.TxnID]*txn{},
		lm:         lm,
		r:          nil,
		txnCounter: atomic.Int64{},
		mut:        &sync.Mutex{},
		pool:       pool,
	}
}

func (t *TxnManagerImpl) Begin() transaction.Transaction {
	t.mut.Lock()
	defer t.mut.Unlock()

	id := t.txnCounter.Add(1)
	txn := txn{id: transaction.TxnID(id)}
	t.actives[txn.GetID()] = &txn
	return &txn
}

func (t *TxnManagerImpl) Commit(transaction transaction.Transaction) {
	t.CommitByID(transaction.GetID())
}

func (t *TxnManagerImpl) AsyncCommit(transaction transaction.Transaction) {
	t.mut.Lock()
	defer t.mut.Unlock()

	txn := t.actives[transaction.GetID()]
	t.lm.AppendLog(wal.NewCommitLogRecord(transaction.GetID(), txn.freedPages))
	delete(t.actives, transaction.GetID())
}

func (t *TxnManagerImpl) Abort(transaction transaction.Transaction) {
	t.AbortByID(transaction.GetID())
}

func (t *TxnManagerImpl) CommitByID(id transaction.TxnID) {
	t.mut.Lock()
	txn := t.actives[id]
	t.mut.Unlock()

	t.lm.WaitAppendLog(wal.NewCommitLogRecord(id, txn.freedPages))
	// IMPORTANT NOTE: if a checkpoint begins right at this line commit log record is persisted but active txn table
	// still includes this log record. Hence, in undo phase there might seem commit log records. In that case that
	// txn should not be rolled back.
	t.mut.Lock()
	delete(t.actives, id)
	for _, page := range txn.freedPages {
		if err := t.pool.FreePage(txn, page, true); err != nil {
			panic(err)
		}
	}
	t.lm.AppendLog(wal.NewTxnEndLogRecord(id))
	t.mut.Unlock()
}

func (t *TxnManagerImpl) AbortByID(id transaction.TxnID) {
	// 1. create an iterator on logs that will iterate a transaction's logs in reverse order
	// 2. create clr logs that are basically logical negations of corresponding logs
	// 3. apply clr records and append them to wal
	// 4. append abort log

	logs := wal.NewTxnLogIterator(id, nil)
	for {
		lr, err := logs.Prev()
		if err != nil {
			// TODO: what to do?
			panic(err)
		}

		if lr == nil {
			// if logs are finished it is rolled back
			break
		}

		if err := t.r.Undo(lr); err != nil {
			panic(err)
		}
	}
}

func (t *TxnManagerImpl) BlockAllTransactions() {
	t.mut.Lock()
}

func (t *TxnManagerImpl) ResumeTransactions() {
	t.mut.Unlock()
}

func (t *TxnManagerImpl) ActiveTransactions() []transaction.TxnID {
	res := make([]transaction.TxnID, 0, len(t.actives))
	for id := range t.actives {
		res = append(res, id)
	}
	return res
}
