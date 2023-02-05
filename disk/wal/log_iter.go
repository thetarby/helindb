package wal

import (
	"encoding/json"
	"errors"
	"helin/disk/pages"
	"helin/transaction"
	"io"
)

var ErrIteratorAtBeginning = errors.New("iterator is at the beginning")
var ErrIteratorAtLast = errors.New("iterator is at the last")
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
	lr, err := it.Curr()
	if err != nil {
		return err
	}
	if lr.TxnID == txn {
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
