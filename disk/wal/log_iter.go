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
			if err == ErrIteratorAtBeginning {
				return nil
			}

			return err
		}
	}
}

func PrevToType(it LogIterator, t LogRecordType) error {
	lr, err := it.Curr()
	if err != nil && err != ErrIteratorNotInitialized {
		return err
	}
	if err != ErrIteratorNotInitialized && lr.Type() == t {
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
	if err != nil && err != ErrIteratorNotInitialized {
		return err
	}
	if err != ErrIteratorNotInitialized && lr.TxnID == txn {
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
	if err != nil && err != ErrIteratorNotInitialized {
		return err
	}
	if err != ErrIteratorNotInitialized && lr.TxnID == txn {
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

// ToJsonLines writes each log as a json line for debugging purposes.
func ToJsonLines(it LogIterator, writer io.Writer) error {
	if err := PrevToStart(it); err != nil {
		return err
	}

	for {
		lr, err := it.Next()
		if err != nil {
			if err == ErrIteratorAtLast {
				return nil
			}
			if err == ErrShortRead {
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
