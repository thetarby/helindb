package transaction

import "sync/atomic"

type Transaction interface {
	GetID() TxnID
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

func (t txnNoop) GetID() TxnID {
	return t.id
}
