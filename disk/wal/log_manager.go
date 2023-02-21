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

type LogManagerInterface interface {
	AppendLog(lr *LogRecord) pages.LSN
}

type LogManager struct {
	// serializer is used to convert between bytes and LogRecord.
	serializer LogRecordSerializer

	currLsn       uint64
	persistentLsn uint64

	bufM sync.Mutex

	gw *GroupWriter
	w  io.Writer
}

func NewLogManager(w io.Writer) *LogManager {
	// TODO: init lsnCounter
	return &LogManager{
		serializer:    &DefaultLogRecordSerializer{area: make([]byte, 0, 100)},
		currLsn:       0,
		persistentLsn: 0,
		bufM:          sync.Mutex{},
		gw:            NewGroupWriter(bufSize, w),
	}
}

// AppendLog appends a log record to wal, set its lsn and return it. This method does not directly flush
// log buffer's content to disk.
func (l *LogManager) AppendLog(lr *LogRecord) pages.LSN {
	l.bufM.Lock()
	defer l.bufM.Unlock()

	lr.Lsn = pages.LSN(atomic.AddUint64(&l.currLsn, 1))

	l.serializer.Serialize(lr, l.gw)
	return lr.Lsn
}

// WaitAppendLog is same as AppendLog, but it waits until appended log is flushed. It can be useful to make sure that
// commit log record is persisted before returning.
func (l *LogManager) WaitAppendLog(lr *LogRecord) pages.LSN {
	l.bufM.Lock()

	lr.Lsn = pages.LSN(atomic.AddUint64(&l.currLsn, 1))

	l.serializer.Serialize(lr, l.gw)
	l.bufM.Unlock()

	l.gw.flushEvent.Wait()
	return lr.Lsn
}

func (l *LogManager) RunFlusher() {
	l.gw.RunFlusher()
}

func (l *LogManager) StopFlusher() error {
	return l.gw.StopFlusher()
}

// Flush is an atomic operation that swaps logBuf and flushBuf followed by an fsync flushBuf.
func (l *LogManager) Flush() error {
	l.bufM.Lock()
	defer l.bufM.Unlock()

	return l.gw.SwapAndWaitFlush()
}

// GetFlushedLSN returns latest lsn persisted to disk.
func (l *LogManager) GetFlushedLSN() pages.LSN {
	return l.gw.latestFlushed
}
