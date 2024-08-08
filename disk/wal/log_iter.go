package wal

import (
	"encoding/json"
	"errors"
	"helin/disk/pages"
	"helin/transaction"
	"io"
)

// ErrIteratorAtBeginning is returned when prev is called when iterator is at the first log record.
var ErrIteratorAtBeginning = errors.New("iterator is at the beginning")

// ErrIteratorAtLast is returned when next is called when iterator is at the latest log record.
var ErrIteratorAtLast = errors.New("iterator is at the last")

// ErrIteratorNotInitialized is returned when curr is called on an iterator that is initialized but
// no next or prev is called yet on it.
var ErrIteratorNotInitialized = errors.New("iterator is not initialized")

// LogIterator is used to move around a log file.
type LogIterator interface {
	Next() (*LogRecord, error)
	Prev() (*LogRecord, error)
	Curr() (*LogRecord, error)
	Skip(lsn pages.LSN) (*LogRecord, error)
}

type BackwardIterator interface {
	Prev() (*LogRecord, error)
}

func PrevToStart(it LogIterator) error {
	for {
		_, err := it.Prev()
		if err != nil {
			if errors.Is(err, ErrIteratorAtBeginning) {
				return nil
			}

			return err
		}
	}
}

func PrevToType(it LogIterator, t LogRecordType) error {
	lr, err := it.Curr()
	if err != nil && !errors.Is(err, ErrIteratorNotInitialized) {
		return err
	}
	if !errors.Is(err, ErrIteratorNotInitialized) && lr.Type() == t {
		return nil
	}

	for {
		lr, err := it.Prev()
		if err != nil {
			return err
		}

		if lr.T == t {
			return nil
		}
	}
}

func PrevToTxn(it LogIterator, txn transaction.TxnID) error {
	lr, err := it.Curr()
	if err != nil && !errors.Is(err, ErrIteratorNotInitialized) {
		return err
	}
	if !errors.Is(err, ErrIteratorNotInitialized) && lr.TxnID == txn {
		return nil
	}

	for {
		lr, err := it.Prev()
		if err != nil {
			return err
		}

		if lr.TxnID == txn {
			return nil
		}
	}
}

// SkipClr should move iterator until clr logs are finished
func SkipClr(it LogIterator) error {
	panic("implement me")
}

// ToJsonLines writes each log as a json line for debugging purposes.
func ToJsonLines(it LogIterator, writer io.Writer) error {
	if err := PrevToStart(it); err != nil {
		return err
	}

	for {
		lr, err := it.Next()
		if err != nil {
			if errors.Is(err, ErrIteratorAtLast) {
				return nil
			}
			return err
		}

		b, err := json.Marshal(lr)
		if err != nil {
			return err
		}

		if _, err := writer.Write(b); err != nil {
			return err
		}
		if _, err := writer.Write([]byte("\n")); err != nil {
			return err
		}
	}
}

// TxnBackwardLogIterator is a LogIterator that iterates only on a transaction's log records.
type TxnBackwardLogIterator struct {
	logIter LogIterator
	init    bool
	txnID   transaction.TxnID
	lsn     pages.LSN // last lsn of the txn
}

func (t *TxnBackwardLogIterator) Prev() (*LogRecord, error) {
	if !t.init {
		lr, err := t.logIter.Skip(t.lsn)
		if err != nil {
			return nil, err
		}

		t.init = true
		return lr, nil
	}

	lr, err := t.logIter.Curr()
	if err != nil {
		return nil, err
	}

	if lr.PrevLsn == pages.ZeroLSN {
		return nil, ErrIteratorAtBeginning
	}

	return t.logIter.Skip(lr.PrevLsn)
}

// NewTxnBackwardLogIterator creates a txn log iterator starting from the latest log record with given txn id.
func NewTxnBackwardLogIterator(txn transaction.Transaction, iter LogIterator) *TxnBackwardLogIterator {
	return &TxnBackwardLogIterator{
		logIter: iter,
		init:    false,
		txnID:   txn.GetID(),
		lsn:     txn.GetPrevLsn(),
	}
}

type TxnBackwardLogIteratorSkipClr struct {
	it *TxnBackwardLogIterator
}

func (t *TxnBackwardLogIteratorSkipClr) Prev() (*LogRecord, error) {
	lr, err := t.it.Prev()
	if err != nil {
		return nil, err
	}

	if lr.IsClr {
		if lr.UndoNext == pages.ZeroLSN {
			return nil, ErrIteratorAtBeginning
		}

		return t.it.logIter.Skip(lr.UndoNext)
	} else {
		return lr, err
	}
}

func NewTxnBackwardLogIteratorSkipClr(txn transaction.Transaction, iter LogIterator) *TxnBackwardLogIteratorSkipClr {
	return &TxnBackwardLogIteratorSkipClr{it: NewTxnBackwardLogIterator(txn, iter)}
}
