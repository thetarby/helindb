package wal

import (
	"helin/bwal"
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
	"os"
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
	exists, err := common.Exists(dir)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err := os.Mkdir(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	lw, err := bwal.OpenBufferedLogWriter(bufSize, segmentSize, dir)
	if err != nil {
		return nil, err
	}

	lw.RunFlusher()

	return &BWALLogManager{lw: lw, ls: ls}, nil
}

func (lm *BWALLogManager) AppendLog(txn transaction.Transaction, lr *LogRecord) pages.LSN {
	if txn != nil {
		lr.PrevLsn = txn.GetPrevLsn()

		var undoing = LogRecord{}
		if undoingB := txn.GetUndoingLog(); undoingB != nil {
			lr.IsClr = true
			lm.ls.Deserialize(undoingB, &undoing)
			lr.UndoNext = undoing.PrevLsn
		}
	}

	b := lm.ls.Serialize(lr)
	lsn, err := lm.lw.Write(b)
	if err != nil {
		// TODO: do not panic
		panic(err)
	}

	lr.Lsn = pages.LSN(lsn)
	if txn != nil {
		txn.SetPrevLsn(lr.Lsn)
	}

	return lr.Lsn
}

func (lm *BWALLogManager) WaitAppendLog(txn transaction.Transaction, lr *LogRecord) (pages.LSN, error) {
	lsn := lm.AppendLog(txn, lr)

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
