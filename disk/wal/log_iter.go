package wal

import (
	"encoding/json"
	"errors"
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

func NextToTxn(it LogIterator, txn transaction.TxnID) error {
	lr, err := it.Curr()
	if err != nil && !errors.Is(err, ErrIteratorNotInitialized) {
		return err
	}
	if !errors.Is(err, ErrIteratorNotInitialized) && lr.TxnID == txn {
		return nil
	}

	for {
		lr, err := it.Next()
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

// txnLogIterator is a LogIterator that iterates only on a transaction's log records.
type txnLogIterator struct {
	logIter LogIterator
	curr    *LogRecord
	txnID   transaction.TxnID
}

var _ LogIterator = &txnLogIterator{}

func (t *txnLogIterator) Next() (*LogRecord, error) {
	if _, err := t.logIter.Next(); err != nil {
		return nil, err
	}

	if err := NextToTxn(t.logIter, t.txnID); err != nil {
		return nil, err
	}

	curr, err := t.logIter.Curr()
	if err != nil {
		return nil, err
	}

	t.curr = curr
	return curr, nil
}

func (t *txnLogIterator) Prev() (*LogRecord, error) {
	if t.curr == nil {
		if err := PrevToTxn(t.logIter, t.txnID); err != nil {
			return nil, err
		}

		curr, err := t.logIter.Curr()
		if err != nil {
			return nil, err
		}

		t.curr = curr
		return curr, nil
	}

	_, err := t.logIter.Prev()
	if err != nil {
		return nil, err
	}

	if err := PrevToTxn(t.logIter, t.txnID); err != nil {
		return nil, err
	}

	curr, err := t.logIter.Curr()
	if err != nil {
		return nil, err
	}

	t.curr = curr
	return curr, nil
}

func (t *txnLogIterator) Curr() (*LogRecord, error) {
	if t.curr == nil {
		return nil, ErrIteratorNotInitialized
	}

	return t.curr, nil
}

// NewTxnLogIterator creates a txn log iterator starting from the latest log record with given txn id.
func NewTxnLogIterator(id transaction.TxnID, iter LogIterator) LogIterator {
	return &txnLogIterator{
		logIter: iter,
		curr:    nil,
		txnID:   id,
	}
}
