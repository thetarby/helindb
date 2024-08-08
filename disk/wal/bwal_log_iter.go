package wal

import (
	"errors"
	"helin/bwal"
	"helin/disk/pages"
)

// bwalLogIter is a LogIterator implementation that iterates on each log in wal without any magic.
type bwalLogIter struct {
	r     *bwal.BufferedLogReader
	curr  *LogRecord
	serDe LogRecordSerDe
}

func OpenBwalLogIter(segmentSize uint64, dir string, serDe LogRecordSerDe) (LogIterator, error) {
	br := bwal.OpenBufferedLogReader(dir, segmentSize)
	return NewBwalLogIter(br, serDe)
}

func OpenBwalLogIterEnd(segmentSize uint64, dir string, serDe LogRecordSerDe) (LogIterator, error) {
	br := bwal.OpenBufferedLogReader(dir, segmentSize)
	_, err := br.LastLSN()
	if err != nil {
		return nil, err
	}

	return NewBwalLogIter(br, serDe)
}

func NewBwalLogIter(r *bwal.BufferedLogReader, serDe LogRecordSerDe) (LogIterator, error) {
	return &bwalLogIter{
		r:     r,
		curr:  nil,
		serDe: serDe,
	}, nil
}

var _ LogIterator = &bwalLogIter{}

func (i *bwalLogIter) Next() (*LogRecord, error) {
	logb, lsn, err := i.r.Next()
	if err != nil {
		if errors.Is(err, bwal.ErrAtLast) {
			return nil, ErrIteratorAtLast
		}

		return nil, err
	}

	if i.curr == nil {
		i.curr = &LogRecord{}
	}
	i.serDe.Deserialize(logb, i.curr)
	i.curr.Lsn = pages.LSN(lsn)

	return i.curr, nil
}

func (i *bwalLogIter) Prev() (*LogRecord, error) {
	logb, lsn, err := i.r.Prev()
	if err != nil {
		if errors.Is(err, bwal.ErrAtFirst) {
			return nil, ErrIteratorAtBeginning
		}

		return nil, err
	}

	if i.curr == nil {
		i.curr = &LogRecord{}
	}
	i.serDe.Deserialize(logb, i.curr)
	i.curr.Lsn = pages.LSN(lsn)

	return i.curr, nil
}

func (i *bwalLogIter) Curr() (*LogRecord, error) {
	if i.curr == nil {
		// TODO: this is problematic when underlying wal iterator is initialized but not this one
		return nil, ErrIteratorNotInitialized
	}

	return i.curr, nil
}

func (i *bwalLogIter) Skip(lsn pages.LSN) (*LogRecord, error) {
	logb, err := i.r.SkipToLSN(uint64(lsn))
	if err != nil {
		if errors.Is(err, bwal.ErrAtFirst) {
			return nil, ErrIteratorAtBeginning
		}

		return nil, err
	}

	if i.curr == nil {
		i.curr = &LogRecord{}
	}

	i.serDe.Deserialize(logb, i.curr)
	i.curr.Lsn = lsn

	return i.curr, nil
}
