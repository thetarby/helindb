package pfreelistv1

import (
	"errors"
	"helin/common"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/freelist/freelistv1"
	"helin/transaction"
)

var _ freelistv1.FreeListPage = &loggedFreelistPage{}

type loggedFreelistPage struct {
	sp   pages.SlottedPage
	pool Pool
	lm   wal.LogManager
	txn  transaction.Transaction
}

func (s *loggedFreelistPage) Get() []byte {
	return s.sp.GetAt(0)
}

func (s *loggedFreelistPage) Set(txn transaction.Transaction, bytes []byte, l *freelistv1.OpLog) error {
	var lr *wal.LogRecord
	switch l.Op {
	case freelistv1.OpPop:
		lr = wal.NewAllocPageLogRecord(txn.GetID(), 0, bytes, common.Clone(s.Get()), l.PageID)
	case freelistv1.OpAdd:
		lr = wal.NewFreePageLogRecord(txn.GetID(), 0, bytes, common.Clone(s.Get()), l.PageID)
	case freelistv1.OpAddSet:
		lr = wal.NewFreePageSetLogRecord(txn.GetID(), 0, bytes, common.Clone(s.Get()), l.PageID)
	default:
		return errors.New("unknown operation")
	}

	if err := s.sp.SetAt(0, bytes); err != nil {
		return err
	}

	s.sp.SetPageLSN(s.lm.AppendLog(txn, lr))

	return nil
}

func (s *loggedFreelistPage) GetPageId() uint64 {
	return s.sp.GetPageId()
}

func (s *loggedFreelistPage) GetLSN() uint64 {
	return uint64(s.sp.GetPageLSN())
}

func (s *loggedFreelistPage) Release() {
	s.pool.Unpin(s.sp.GetPageId(), true)
	// s.sp.WUnlatch()
	s.txn.ReleaseLatch(s.sp.GetPageId())
}

func newLoggedFreelistPage(p *pages.RawPage, pool Pool, lm wal.LogManager, txn transaction.Transaction) *loggedFreelistPage {
	return &loggedFreelistPage{sp: pages.CastSlottedPage(p), pool: pool, lm: lm, txn: txn}
}

func initLoggedFreelistPage(txn transaction.Transaction, p *pages.RawPage, pool Pool, lm wal.LogManager) *loggedFreelistPage {
	lsn := lm.AppendLog(txn, wal.NewPageFormatLogRecord(txn.GetID(), pages.TypeSlottedPage, p.GetPageId()))

	sp := pages.InitSlottedPage(p)
	sp.SetPageLSN(lsn)
	sp.SetDirty()

	return &loggedFreelistPage{sp: sp, pool: pool, lm: lm, txn: txn}
}
