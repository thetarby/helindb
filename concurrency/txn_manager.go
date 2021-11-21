package concurrency

type IsolationLevel int

const (
	READ_UNCOMMITTED IsolationLevel = iota
	READ_COMMITTED
	REPEATABLE_READ
) 

type ITxnManager interface{
	Begin(IsolationLevel) Transaction
	Commit(Transaction)
	Abort(Transaction)
	BlockAllTransactions()
	ResumeTransactions()
}