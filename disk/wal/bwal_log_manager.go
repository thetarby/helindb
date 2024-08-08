package wal

import (
	"helin/bwal"
	"helin/disk/pages"
)

type LogRecordSerDe interface {
	Serialize(lr *LogRecord) []byte
	Deserialize(d []byte, lr *LogRecord)
}

type BWALLogManager struct {
	lw *bwal.BufferedLogWriter
	ls LogRecordSerDe
}

func OpenBWALLogManager(bufSize int, segmentSize uint64, dir string, ls LogRecordSerDe) (*BWALLogManager, error) {
	lw, err := bwal.OpenBufferedLogWriter(bufSize, segmentSize, dir)
	if err != nil {
		return nil, err
	}

	lw.RunFlusher()

	return &BWALLogManager{lw: lw, ls: ls}, nil
}

func (lm *BWALLogManager) AppendLog(lr *LogRecord) pages.LSN {
	b := lm.ls.Serialize(lr)
	lsn, err := lm.lw.Write(b)
	if err != nil {
		// TODO: do not panic
		panic(err)
	}

	lr.Lsn = pages.LSN(lsn)

	return lr.Lsn
}

func (lm *BWALLogManager) WaitAppendLog(lr *LogRecord) (pages.LSN, error) {
	lsn := lm.AppendLog(lr)

	if err := lm.lw.Wait(uint64(lr.Lsn)); err != nil {
		return lsn, err
	}

	return lsn, nil
}

func (lm *BWALLogManager) GetFlushedLSNOrZero() pages.LSN {
	lsn, err := lm.lw.GetFlushedLSN()
	if err != nil {
		return pages.ZeroLSN
	}

	return pages.LSN(lsn)
}

func (lm *BWALLogManager) Flush() error {
	return lm.lw.Flush()
}

var _ LogManager = &BWALLogManager{}
