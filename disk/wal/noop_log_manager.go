package wal

import (
	"helin/disk/pages"
)

var NoopLM = &noopLM{}

type noopLM struct{}

func (n *noopLM) AppendLog(lr *LogRecord) pages.LSN {
	return pages.ZeroLSN
}

func (n *noopLM) WaitAppendLog(lr *LogRecord) (pages.LSN, error) {
	return pages.ZeroLSN, nil
}

func (n *noopLM) GetFlushedLSNOrZero() pages.LSN {
	return pages.ZeroLSN
}

func (n *noopLM) Flush() error {
	return nil
}

var _ LogManager = &noopLM{}
