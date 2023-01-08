package concurrency

import (
	"helin/disk/wal"
	"helin/transaction"
	"sync"
	"sync/atomic"
)

type IsolationLevel int

const (
	ReadUncommitted IsolationLevel = iota
	ReadCommitted
	RepeatableRead
)

type txn struct {
	id transaction.TxnID
}

func (t txn) GetID() transaction.TxnID {
	return t.id
}

type TxnManager interface {
	Begin(IsolationLevel) transaction.Transaction
	Commit(transaction.Transaction)
	Abort(transaction.Transaction)
	CommitByID(transaction.TxnID)
	AbortByID(id transaction.TxnID)

	BlockAllTransactions()
	ResumeTransactions()

	ActiveTransactions() []transaction.TxnID
}

var _ TxnManager = &TxnManagerImpl{}

type TxnManagerImpl struct {
	actives    map[transaction.TxnID]bool
	r          *Recovery
	txnCounter atomic.Int64
	mut        *sync.Mutex
}

func NewTxnManagerImpl() *TxnManagerImpl {
	return &TxnManagerImpl{
		actives:    map[transaction.TxnID]bool{},
		r:          nil,
		txnCounter: atomic.Int64{},
		mut:        &sync.Mutex{},
	}
}

func (t *TxnManagerImpl) Begin(level IsolationLevel) transaction.Transaction {
	id := t.txnCounter.Add(1)
	return txn{id: transaction.TxnID(id)}
}

func (t *TxnManagerImpl) Commit(transaction transaction.Transaction) {
	//TODO implement me
	panic("implement me")
}

func (t *TxnManagerImpl) Abort(transaction transaction.Transaction) {
	t.AbortByID(transaction.GetID())
}

func (t *TxnManagerImpl) CommitByID(id transaction.TxnID) {
	//TODO implement me
	panic("implement me")
}

func (t *TxnManagerImpl) AbortByID(id transaction.TxnID) {
	// 1. create an iterator on logs that will iterate a transaction's logs in reverse order
	// 2. create clr logs that are basically logical negations of corresponding logs
	// 3. apply clr records and append them to wal
	// 4. append abort log

	logs := wal.NewTxnLogIterator(id)
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
	//TODO implement me
	panic("implement me")
}
