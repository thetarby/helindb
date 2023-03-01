package wal

import (
	"helin/disk/pages"
	"io"
	"sync"
	"sync/atomic"
)

const (
	bufSize = 1024 * 64
)

/*
	TODO: make log manager interface
*/

type LogManager interface {
	// AppendLog appends a log record to wal, set its lsn and return it. This method does not directly flush
	// log buffer's content to disk.
	AppendLog(lr *LogRecord) pages.LSN

	// WaitAppendLog is same as AppendLog, but it waits until appended log is flushed.
	WaitAppendLog(lr *LogRecord) pages.LSN

	// GetFlushedLSN returns the latest log's lsn that is flushed to underlying io.Writer.
	GetFlushedLSN() pages.LSN

	// Flush is an atomic operation that swaps logBuf and flushBuf followed by an fsync of flushBuf.
	Flush() error
}

type LogManagerImpl struct {
	// serializer is used to convert between bytes and LogRecord.
	serializer LogRecordSerializer

	currLsn       uint64
	persistentLsn uint64

	bufM sync.Mutex

	gw *GroupWriter
	w  io.Writer
}

func NewLogManager(w io.Writer) *LogManagerImpl {
	// TODO: init lsnCounter
	return &LogManagerImpl{
		serializer:    &DefaultLogRecordSerializer{area: make([]byte, 0, 100)},
		currLsn:       0,
		persistentLsn: 0,
		bufM:          sync.Mutex{},
		gw:            NewGroupWriter(bufSize, w),
	}
}

func (l *LogManagerImpl) AppendLog(lr *LogRecord) pages.LSN {
	l.bufM.Lock()
	defer l.bufM.Unlock()

	lr.Lsn = pages.LSN(atomic.AddUint64(&l.currLsn, 1))

	l.serializer.Serialize(lr, l.gw)
	return lr.Lsn
}

func (l *LogManagerImpl) WaitAppendLog(lr *LogRecord) pages.LSN {
	l.bufM.Lock()

	lr.Lsn = pages.LSN(atomic.AddUint64(&l.currLsn, 1))

	l.serializer.Serialize(lr, l.gw)
	l.bufM.Unlock()

	l.gw.flushEvent.Wait()
	return lr.Lsn
}

func (l *LogManagerImpl) RunFlusher() {
	l.gw.RunFlusher()
}

func (l *LogManagerImpl) StopFlusher() error {
	return l.gw.StopFlusher()
}

func (l *LogManagerImpl) Flush() error {
	l.bufM.Lock()
	defer l.bufM.Unlock()

	return l.gw.SwapAndWaitFlush()
}

func (l *LogManagerImpl) GetFlushedLSN() pages.LSN {
	return l.gw.latestFlushed
}
