package bwal

import (
	"errors"
	"fmt"
	"io"
)

var ErrAtLast = errors.New("tail of wal is reached")
var ErrAtFirst = errors.New("head of wal is reached")
var errPartialLog = errors.New("partial log record found")

type BufferedLogReader struct {
	segmentReader       SegmentReader
	currLSN             *uint64
	currLogRecordHeader *LogRecordHeader
}

func OpenBufferedLogReader(dir string, segmentSize uint64) *BufferedLogReader {
	sr, err := NewSegmentFS().OpenSegmentReader(dir, segmentSize)
	if err != nil {
		panic(err)
	}

	return &BufferedLogReader{segmentReader: sr}
}

func (r *BufferedLogReader) SkipToLSN(lsn uint64) ([]byte, error) {
	_, err := r.segmentReader.Seek(int64(lsn), io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek error: %w", err)
	}

	// TODO: no alloc
	logRecordHeaderB := make([]byte, LogRecordHeaderSize)
	n, err := r.segmentReader.Read(logRecordHeaderB)
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, ErrAtLast
		}

		return nil, fmt.Errorf("failed to read log record header: %w, %w", err, errPartialLog)
	}
	if n != LogRecordHeaderSize {
		return nil, errors.New("short read")
	}

	logRecordHeader := readLogRecordHeader(logRecordHeaderB)
	res := make([]byte, logRecordHeader.Size)

	n, err = r.segmentReader.Read(res)
	if err != nil {
		return nil, fmt.Errorf("failed to read log record: %w, %w", err, errPartialLog)
	}
	if n != len(res) {
		return nil, errors.New("short read")
	}

	r.currLSN = &lsn
	r.currLogRecordHeader = &logRecordHeader
	return res, nil
}

func (r *BufferedLogReader) Next() ([]byte, uint64, error) {
	nextLSN := uint64(0)
	if r.currLSN != nil {
		nextLSN = *r.currLSN + uint64(r.currLogRecordHeader.Size) + uint64(LogRecordHeaderSize)
	} else {
		s, err := r.segmentReader.FrontTruncatedSize()
		if err != nil {
			return nil, 0, err
		}

		nextLSN = s
	}

	l, err := r.SkipToLSN(nextLSN)
	return l, nextLSN, err
}

func (r *BufferedLogReader) Prev() ([]byte, uint64, error) {
	if r.currLSN == nil {
		return nil, 0, ErrAtFirst // TODO is this OK?
	}
	if *r.currLSN == 0 {
		return nil, 0, ErrAtFirst
	}

	prevLSN := r.currLogRecordHeader.PrevLSN
	l, err := r.SkipToLSN(prevLSN)
	if err != nil {
		if errors.Is(err, ErrOutOfBounds) {
			return nil, 0, ErrAtFirst
		}

		return nil, 0, err
	}

	return l, prevLSN, nil
}

func (r *BufferedLogReader) Reset() {
	r.currLSN = nil
	r.currLogRecordHeader = nil
}

func (r *BufferedLogReader) LastLSN() ([]byte, error) {
	h, err := r.segmentReader.LastSegmentHeader()
	if err != nil {
		return nil, fmt.Errorf("failed to get last segment's header: %w", err)
	}

	record, err := r.SkipToLSN(h.LastLSN)
	if err != nil {
		return nil, fmt.Errorf("failed to get last lsn of last segment: %w", err)
	}

	for {
		curr, _, err := r.Next()
		if err != nil {
			if errors.Is(err, ErrAtLast) {
				break
			}

			return nil, fmt.Errorf("failed while iterating to next: %w", err)
		}

		record = curr
	}

	return record, nil
}

func (r *BufferedLogReader) RepairWAL() error {
	defer r.Reset()

	_, err := r.LastLSN()
	if err == nil || errors.Is(err, ErrNoSegmentFile) {
		return nil
	}

	if errors.Is(err, errPartialLog) {
		// since next has failed currLSN is still the one before the partial record, meaning it is the last
		// record that is fully written.
		eof := *r.currLSN + uint64(r.currLogRecordHeader.Size) + uint64(LogRecordHeaderSize)
		if err := r.segmentReader.Truncate(eof); err != nil {
			return fmt.Errorf("failed to truncate wal, err: %w", err)
		}

		return nil
	}

	// NOTE: what to do when error is ErrUninitializedSegmentFile? (should not happen ideally since it is repaired
	// automatically when opening segments)

	// this should not happen unless there is an occasional io error (that can be retried) or something catastrophic happened.
	return fmt.Errorf("wal cannot be repaired: %w", err)
}

func (r *BufferedLogReader) TruncateUntil(lsn uint64) error {
	return r.segmentReader.TruncateFront(lsn)
}
