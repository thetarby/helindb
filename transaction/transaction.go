package transaction

type Transaction interface {
	GetID() TxnID
}

func TxnTODO() Transaction {
	return TxnNoop()
}

type TxnID uint64

func TxnNoop() Transaction {
	return txnNoop{}
}

var _ Transaction = &txnNoop{}

type txnNoop struct{}

func (t txnNoop) GetID() TxnID {
	return 1
}
