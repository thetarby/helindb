package transaction

import (
	"helin/disk/pages"
	"sync/atomic"
)

type Transaction interface {
	GetID() TxnID
	FreePage(pageID uint64)
	SetPrevLsn(pages.LSN)
	GetPrevLsn() pages.LSN
	GetUndoingLog() []byte
	SetUndoingLog([]byte)
}

func TxnTODO() Transaction {
	return TxnNoop()
}

type TxnID uint64

var noOpTxnCounter uint64 = 0

func TxnNoop() Transaction {
	id := atomic.AddUint64(&noOpTxnCounter, 1)
	return txnNoop{
		id: TxnID(id),
	}
}

var _ Transaction = &txnNoop{}

type txnNoop struct {
	id TxnID
}

func (t txnNoop) GetUndoingLog() []byte {
	return nil
}

func (t txnNoop) SetUndoingLog([]byte) {
	return
}

func (t txnNoop) SetPrevLsn(lsn pages.LSN) {
	return
}

func (t txnNoop) GetPrevLsn() pages.LSN {
	return pages.ZeroLSN
}

func (t txnNoop) FreePage(pageID uint64) {
	return
}

func (t txnNoop) GetID() TxnID {
	return t.id
}
