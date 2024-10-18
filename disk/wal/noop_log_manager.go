package wal

import (
	"helin/disk/pages"
	"helin/transaction"
	"sync/atomic"
)

var NoopLM = &noopLM{}

type noopLM struct {
	lsn atomic.Uint64
}

func (n *noopLM) AppendLog(txn transaction.Transaction, lr *LogRecord) pages.LSN {
	return pages.LSN(n.lsn.Add(1))
}

func (n *noopLM) WaitAppendLog(txn transaction.Transaction, lr *LogRecord) (pages.LSN, error) {
	return pages.LSN(n.lsn.Add(1)), nil
}

func (n *noopLM) Wait(lsn pages.LSN) error {
	return nil
}

func (n *noopLM) GetFlushedLSNOrZero() pages.LSN {
	return pages.LSN(n.lsn.Load())
}

func (n *noopLM) Flush() error {
	return nil
}

var _ LogManager = &noopLM{}
