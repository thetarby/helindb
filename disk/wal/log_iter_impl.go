package wal

import (
	"helin/transaction"
	"io"
	"os"
)

// logIter is a LogIterator implementation that iterates on each log in wal without any magic.
type logIter struct {
	reader     io.ReadSeeker
	serializer LogRecordSerializer
	lens       []int
	i          int
}

func NewLogIter(reader *os.File, serializer LogRecordSerializer, iteratorType uint8) (LogIterator, error) {
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	var it *logIter
	if iteratorType == 0 {
		// starts from end
		it = &logIter{reader: reader, serializer: serializer}
		i := 0
		for {
			i++
			//println(i)
			//if i == 427971 {
			//	println("sa")
			//}
			_, err := it.Next()
			if err != nil {
				if err == ErrIteratorAtLast {
					break
				}
				if err == ErrShortRead {
					var sum int64 = 0
					for _, i := range it.lens {
						sum += int64(i)
					}

					if err := reader.Truncate(sum); err != nil {
						return nil, err
					}

					err := reader.Sync()
					if err != nil {
						return nil, err
					}

					return it, nil
				}

				return nil, err
			}
		}
	} else if iteratorType == 1 {
		// starts from beginning
		it = &logIter{reader: reader, serializer: serializer}
	}

	return it, nil
}

var _ LogIterator = &logIter{}

func (l *logIter) Next() (*LogRecord, error) {
	rec, n, err := l.serializer.Deserialize(l.reader)
	if err != nil {
		if err == io.EOF {
			return nil, ErrIteratorAtLast
		}
		if err == ErrShortRead {
			if _, err := l.reader.Seek(int64(-n), io.SeekCurrent); err != nil {
				panic(err)
			}

			return nil, ErrShortRead
		}

		return nil, err
	}

	if l.i == len(l.lens) {
		l.lens = append(l.lens, n)
	}

	l.i += 1
	return rec, nil
}

func (l *logIter) Prev() (*LogRecord, error) {
	if l.i == 1 {
		return nil, ErrIteratorAtBeginning
	}

	if _, err := l.reader.Seek(-int64(l.lens[l.i-1]+l.lens[l.i-2]), io.SeekCurrent); err != nil {
		return nil, err
	}

	l.i -= 2

	return l.Next()
}

func (l *logIter) Curr() (*LogRecord, error) {
	if _, err := l.reader.Seek(-int64(l.lens[l.i-1]), io.SeekCurrent); err != nil {
		return nil, err
	}

	l.i -= 1

	return l.Next()
}

// txnLogIterator is a LogIterator that iterates only on a transaction's log records.
type txnLogIterator struct {
	logIter LogIterator
	curr    *LogRecord
	txnID   transaction.TxnID
}

var _ LogIterator = &txnLogIterator{}

func (t *txnLogIterator) Next() (*LogRecord, error) {
	//TODO implement me
	panic("implement me")
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

	//if t.curr.PrevLsn == pages.ZeroLSN {
	//	return nil, ErrIteratorAtBeginning
	//}
	//
	//if err := PrevToLsn(t.logIter, t.curr.PrevLsn); err != nil {
	//	return nil, err
	//}

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

func NewTxnLogIterator(id transaction.TxnID, iter LogIterator) LogIterator {
	return &txnLogIterator{
		logIter: iter,
		curr:    nil,
		txnID:   id,
	}
}
