package concurrency

import (
	"helin/buffer"
	"helin/disk/wal"
)

type CheckpointManager interface {
	TakeCheckpoint() error
}

type CheckpointManagerImpl struct {
	pool       *buffer.BufferPool
	logManager *wal.LogManager
	txnManager TxnManager
}

func (c *CheckpointManagerImpl) TakeCheckpoint() error {
	// 1. take a list of all active transactions
	// 2. write checkpoint begin log with active transaction list
	// 3. flush all dirty pages in buffer pool
	// 4. take new active transactions
	// 5. write checkpoint end log with active transaction list
	// NOTE: active transactions on checkpoint end log might not be necessary

	// write checkpoint begin log with all active transactions that started before checkpoint-begin log record
	c.txnManager.BlockAllTransactions()
	actives := c.txnManager.ActiveTransactions()
	c.logManager.AppendLog(wal.NewCheckpointBeginLogRecord(actives...))
	c.txnManager.ResumeTransactions()

	// flush all dirty pages
	if err := c.pool.FlushAll(); err != nil {
		// TODO: how to handle? maybe log checkpoint failed, or do not log anything?
		return err
	}

	// write checkpoint end
	c.txnManager.BlockAllTransactions()
	actives = c.txnManager.ActiveTransactions()
	c.logManager.AppendLog(wal.NewCheckpointEndLogRecord(actives...))
	c.txnManager.ResumeTransactions()

	return nil
}
