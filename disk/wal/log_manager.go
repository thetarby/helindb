package wal

import (
	"io"
	"sync"
	"sync/atomic"
)

const (
	bufSize = 1024 * 16
)

type LogManager struct {
	// serializer is used to convert between bytes and LogRecord.
	serializer LogRecordSerializer

	nextLsn       atomic.Int64
	persistentLsn atomic.Int64

	bufM sync.Mutex

	gw *GroupWriter
	w  io.Writer
}

func NewLogManager(w io.Writer) *LogManager {
	return &LogManager{
		serializer:    &DefaultLogRecordSerializer{area: make([]byte, 0, 100)},
		nextLsn:       atomic.Int64{},
		persistentLsn: atomic.Int64{},
		bufM:          sync.Mutex{},
		gw:            NewGroupWriter(bufSize, w),
	}
}

func (l *LogManager) AppendLog(lr *LogRecord) {
	l.bufM.Lock()
	defer l.bufM.Unlock()

	l.serializer.Serialize(lr, l.gw)
}

func (l *LogManager) RunFlusher() {
	l.gw.RunFlusher()
}

func (l *LogManager) StopFlusher() error {
	return l.gw.StopFlusher()
}

// Flush is an atomic operation that swaps logBuf and flushBuf followed by an fsync flushBuf.
func (l *LogManager) Flush() error {
	return l.gw.swapAndWaitFlush()
}
