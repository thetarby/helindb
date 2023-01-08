package wal

import (
	"errors"
	"helin/disk/pages"
	"helin/transaction"
)

var ErrIteratorAtBeginning = errors.New("iterator is at the beginning")
var ErrIteratorNotInitialized = errors.New("iterator is not initialized")

// LogIterator is used to move around a log file.
type LogIterator interface {
	Next() (*LogRecord, error)
	Prev() (*LogRecord, error)
	Curr() (*LogRecord, error)
}

func PrevToType(it LogIterator, t LogRecordType) error {
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

func PrevToLsn(it LogIterator, lsn pages.LSN) error {
	for {
		lr, err := it.Prev()
		if err != nil {
			return err
		}

		if lr.Lsn == lsn {
			return nil
		}
	}
}

func PrevToTxn(it LogIterator, txn transaction.TxnID) error {
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

func NextToType(it LogIterator, t LogRecordType) error {
	for {
		lr, err := it.Next()
		if err != nil {
			return err
		}

		if lr.T == t {
			return nil
		}
	}
}

func NextToLsn(it LogIterator, lsn pages.LSN) error {
	for {
		lr, err := it.Next()
		if err != nil {
			return err
		}

		if lr.Lsn == lsn {
			return nil
		}
	}
}
