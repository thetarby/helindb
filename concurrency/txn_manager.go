package concurrency

import (
	"helin/disk/wal"
	"helin/transaction"
	"sync"
	"sync/atomic"
)

type txn struct {
	id transaction.TxnID
}

func (t txn) GetID() transaction.TxnID {
	return t.id
}

// TxnManager keeps track of running transactions.
type TxnManager interface {
	Begin() transaction.Transaction
	Commit(transaction.Transaction)
	CommitByID(transaction.TxnID)
	Abort(transaction.Transaction)
	AbortByID(id transaction.TxnID)

	BlockAllTransactions()
	ResumeTransactions()

	ActiveTransactions() []transaction.TxnID
}

var _ TxnManager = &TxnManagerImpl{}

type TxnManagerImpl struct {
	actives    map[transaction.TxnID]transaction.Transaction
	lm         *wal.LogManager
	r          *Recovery
	txnCounter atomic.Int64
	mut        *sync.Mutex
}

func NewTxnManager(lm *wal.LogManager) *TxnManagerImpl {
	return &TxnManagerImpl{
		actives:    map[transaction.TxnID]transaction.Transaction{},
		lm:         lm,
		r:          nil,
		txnCounter: atomic.Int64{},
		mut:        &sync.Mutex{},
	}
}

func (t *TxnManagerImpl) Begin() transaction.Transaction {
	t.mut.Lock()
	defer t.mut.Unlock()

	id := t.txnCounter.Add(1)
	txn := txn{id: transaction.TxnID(id)}
	t.actives[txn.GetID()] = txn
	return txn
}

func (t *TxnManagerImpl) Commit(transaction transaction.Transaction) {
	t.CommitByID(transaction.GetID())
}

func (t *TxnManagerImpl) Abort(transaction transaction.Transaction) {
	t.AbortByID(transaction.GetID())
}

func (t *TxnManagerImpl) CommitByID(id transaction.TxnID) {
	t.mut.Lock()
	defer t.mut.Unlock()

	t.lm.AppendLog(wal.NewCommitLogRecord(id))
	delete(t.actives, id)
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
