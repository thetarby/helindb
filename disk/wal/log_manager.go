package wal

import (
	"helin/disk/pages"
)

type LogManager interface {
	// AppendLog appends a log record to wal, set its lsn and return it. This method does not directly flush
	// log buffer's content to disk.
	AppendLog(lr *LogRecord) pages.LSN

	// WaitAppendLog is same as AppendLog, but it waits until appended log is flushed.
	WaitAppendLog(lr *LogRecord) (pages.LSN, error)

	// GetFlushedLSNOrZero returns the latest log's lsn that is flushed to underlying io.Writer.
	GetFlushedLSNOrZero() pages.LSN

	// Flush is an atomic operation that swaps logBuf and flushBuf followed by an fsync of flushBuf.
	Flush() error
}
