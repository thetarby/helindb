package freelist

//var _ wal.LogManager = &inMemLM{}
//
//type inMemLM struct {
//	logs []wal.LogRecord
//	lsn  pages.LSN
//}
//
//func (i *inMemLM) AppendLog(lr *wal.LogRecord) pages.LSN {
//	i.logs = append(i.logs, *lr)
//	i.lsn++
//	return i.lsn
//}
//
//func (i *inMemLM) WaitAppendLog(lr *wal.LogRecord) pages.LSN {
//	return i.AppendLog(lr)
//}
//
//func (i *inMemLM) GetFlushedLSN() pages.LSN {
//	return i.lsn
//}
//
//func (i *inMemLM) Flush() error {
//	return nil
//}
//
//func TestRecovery(t *testing.T) {
//	dbFile := uuid.New().String()
//	defer os.Remove(dbFile)
//
//	lm := &inMemLM{}
//	bp := buffer.NewBufferPool(dbFile, 128)
//	fl := NewFreeList(bp, lm, true)
//
//	for i := 0; i < 10; i++ {
//		fl.Add(transaction.TxnNoop(), uint64(i))
//	}
//
//	for _, log := range lm.logs {
//		if fl.GetHeaderPageLsn() < log.Lsn {
//			fl.Add(transaction.TxnNoop(), log.PageID)
//		}
//	}
//}
