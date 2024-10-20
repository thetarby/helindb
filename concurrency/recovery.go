package concurrency

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"helin/buffer"
	"helin/common"
	"helin/disk"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/freelist/freelistv1"
	"helin/locker"
	"helin/transaction"
	"io"
	"log"
)

// Recovery encapsulates logic about coming back from a crash. It brings database file to its latest stable state.
type Recovery struct {
	logs       wal.LogIterator
	logManager wal.LogManager
	dm         RecoveryDiskManager
	locker     *locker.LockManager
}

func NewRecovery(iter wal.LogIterator, lm wal.LogManager, dm *disk.Manager, pool buffer.Pool) *Recovery {
	return &Recovery{
		logs:       iter,
		logManager: lm,
		dm: &recoveryDiskManager{
			dm:   dm,
			pool: pool,
		},
		locker: locker.NewLockManager(),
	}
}

// Recover is called to come back from failures. Brings database file to its latest correct state.
func (r *Recovery) Recover() error {
	// 1. iterate over logs and locate the latest successful checkpoint's begin record
	// 2. redo all log records until the end of iterator to bring database file to the state when crash occurred
	// 3. populate active transactions by keeping track of commit, abort and begin log records
	// 4. rollback all transactions in active transactions list
	if err := wal.PrevToType(r.logs, wal.TypeCheckpointEnd); err != nil {
		return err
	}

	chEndLr, err := r.logs.Curr()
	if err != nil {
		return err
	}

	chBeginLr, err := r.logs.Skip(chEndLr.PrevLsn)
	if err != nil {
		return err
	}

	common.Assert(chBeginLr.Type() == wal.TypeCheckpointBegin, "corrupt checkpoint records")

	// get active transactions
	txns := make(map[transaction.TxnID]*txn)
	activeTxn := make(map[transaction.TxnID]string)

	for _, active := range chBeginLr.Actives {
		activeTxn[active] = "undo"
		txns[active] = newTxn(active, nil, pages.ZeroLSN, r.locker)
	}

	if len(chBeginLr.Actives) > 0 {
		minTxn := chBeginLr.Actives[0]
		for _, active := range chBeginLr.Actives {
			if active < minTxn {
				minTxn = active
			}
		}

		common.Assert(pages.LSN(minTxn) < chBeginLr.Lsn, "corrupted transaction id")

		// get cursor to txn starter
		_, err := r.logs.Skip(pages.LSN(minTxn))
		if err != nil {
			return err
		}
	}

	// redo all logs until log end and populate active txn map
	for {
		lr, err := r.logs.Next()
		if err != nil {
			if errors.Is(err, wal.ErrIteratorAtLast) {
				break
			}

			return fmt.Errorf("next failed: %w", err)
		}

		if common.OneOf(lr.Type(), wal.TypeCheckpointEnd, wal.TypeCheckpointBegin) {
			continue
		}

		txnID := lr.GetTxnID()

		// populate transactions map if newly encountered
		if _, ok := txns[txnID]; !ok {
			txns[txnID] = newTxn(txnID, nil, lr.Lsn, r.locker)
		}

		txns[txnID].SetPrevLsn(lr.Lsn)

		if lr.Type() == wal.TypeTxnEnd {
			delete(activeTxn, txnID)
			delete(txns, txnID)
			continue
		} else if lr.Type() == wal.TypeCommit {
			activeTxn[txnID] = "commit"
			txns[txnID].setFreedPages(lr.FreedPages)
			continue
		} else {
			activeTxn[txnID] = "undo"
		}

		if err := r.Redo(lr); err != nil {
			return fmt.Errorf("redo error %w", err)
		}
	}

	log.Println("active transactions during crash:")
	for id, status := range activeTxn {
		log.Printf("active txn: %v, status: %v\n", id, status)
	}

	// Complete all transactions that are active and committed.
	for id, status := range activeTxn {
		if status == "commit" {
			if err := r.CompleteTxn(txns[id]); err != nil {
				return fmt.Errorf("redo complete error %w", err)
			}
		}
	}

	// Abort all active transactions that are active and uncommitted when crash occurred.
	// This essentially will undo all logs generated by these transactions.
	for id, status := range activeTxn {
		if status == "undo" {
			r.RollbackTxn(txns[id])
		}
	}

	return nil
}

// RollbackTxn rolls back all the changes reflected by transaction's log records.
func (r *Recovery) RollbackTxn(txn transaction.Transaction) {
	// 1. create an iterator on logs that will iterate a transaction's logs in reverse order
	// 2. create clr logs that are basically logical negations of corresponding logs
	// 3. apply clr records and append them to wal
	// 4. append abort log

	logs := wal.NewTxnBackwardLogIteratorSkipClr(txn, r.logs)
	for {
		lr, err := logs.Prev()
		if err != nil {
			if errors.Is(err, wal.ErrIteratorAtBeginning) {
				break
			}
		}

		if lr == nil {
			// if logs are finished it is rolled back
			break
		}

		common.Assert(lr.Type() != wal.TypeCommit, "rollback called on committed txn: %v", txn.GetID())
		common.Assert(lr.Type() != wal.TypeFreePage, "free page cannot be undone txn: %v", txn.GetID())
		common.Assert(lr.Type() != wal.TypeFreePageSet, "free page cannot be undone txn: %v", txn.GetID())

		txn.SetUndoingLog(lr.Raw)
		if err := r.Undo(txn, lr); err != nil {
			panic(err)
		}
	}

	// r.logManager.AppendLog(txn, wal.NewAbortLogRecord(txn.GetID()))
	r.logManager.AppendLog(txn, wal.NewTxnEndLogRecord(txn.GetID()))
}

// CompleteTxn completes a committed transaction that has some incomplete pending actions
// (it only includes freeing pages for now).
func (r *Recovery) CompleteTxn(txn transaction.Transaction) error {
	logs := wal.NewTxnBackwardLogIterator(txn, r.logs)

	toFree := map[uint64]bool{}
	var freed []uint64

	for {
		lr, err := logs.Prev()
		if err != nil {
			return err
		}

		if lr.Type() == wal.TypeCommit {
			for _, page := range lr.FreedPages {
				toFree[page] = true
			}

			for _, page := range freed {
				delete(toFree, page)
			}

			break
		}

		// assertions
		common.Assert(lr.Type() == wal.TypeFreePage || lr.Type() == wal.TypeFreePageSet, "encountered a log record other than TypeFreePage after TypeCommit")
		common.Assert(toFree[lr.PageID], "txn freed a page that is not in FreedPages")

		// no need to do anything when TypeFreePageSet
		if lr.Type() == wal.TypeFreePage {
			// assertions
			common.Assert(toFree[lr.PageID], "txn freed a page that is not in FreedPages")
			common.Assert(r.dm.GetFreeListLsn(txn) < lr.Lsn, "complete txn encountered a log record that is not redone")

			freed = append(freed, lr.PageID)
		}
	}

	// 3. for all remaining pages in toFree, free all pages and append corresponding TypeFreePage log records.
	for page := range toFree {
		if err := r.dm.FreePage(txn, page); err != nil {
			return err
		}
	}

	// 4. append end record and complete txn
	r.logManager.AppendLog(txn, wal.NewTxnEndLogRecord(txn.GetID()))
	return nil
}

// Redo applies change in the log record if page's pageLsn is smaller than log's lsn and updates pageLsn,
// if not, this is a noop since changes are already on the page.
func (r *Recovery) Redo(lr *wal.LogRecord) error {
	if common.OneOf(lr.Type(), wal.TypeTxnStarter, wal.TypeTxnEnd, wal.TypeAbort) {
		return nil
	}

	// if disk allocated
	if lr.Type() == wal.TypeNewPage && !lr.IsPop() {
		// if this is a new page which is not popped from free list, it might not make it to the disk yet, meaning
		// it would return EOF if one tries to read it from disk.
		_, err := r.dm.GetPage(lr.PageID)
		if err != nil {
			if err == io.EOF {
				return r.dm.FormatPage(lr.PageID, lr.Lsn)
			}

			return err
		} else {
			return nil
		}
	}

	// if it is a free page log record, page id represents freed page's id but, we want to modify free list
	// hence fetch free list header page.
	pageID := common.Ternary(lr.Type() == wal.TypeFreePage || (lr.Type() == wal.TypeNewPage && lr.IsPop()), freelistv1.HeaderPageID, lr.PageID)
	p, err := r.dm.GetPage(pageID)
	if err != nil {
		return err
	}
	defer r.dm.Unpin(pageID)

	// NOTE: page is not formatted or initialized at all. this can happen when a page is not synced to file but a page with
	// larger pageID is synced. (seek operation writes zeros in between)
	//if p.GetPageLSN() == 0 {
	//	sp := pages.InitSlottedPage(p)
	//	p = &sp
	//	p.SetDirty()
	//}
	common.Assert(p.GetPageLSN() > 0 || lr.Type() == wal.TypePageFormat, "page lsn is not greater than 0")

	// note that if this is a TypeFreePage log record p.GetPageLSN() returns free list's lsn.
	if p.GetPageLSN() >= lr.Lsn {
		return nil
	}

	// first redo changes
	switch lr.Type() {
	case wal.TypePageFormat:
		if lr.PageType == pages.TypeSlottedPage {
			pages.InitSlottedPage(p)
		} else if lr.PageType == pages.TypeCopyAtPage {
			pages.InitCopyAtPage(p)
		} else {
			return fmt.Errorf("unknown page type while formatting: %v", lr.PageType)
		}
	case wal.TypeInsert:
		sp := convertSlotted(p)
		if err := sp.InsertAt(int(lr.Idx), lr.Payload); err != nil {
			return err
		}
	case wal.TypeDelete:
		sp := convertSlotted(p)
		if d := sp.GetAt(int(lr.Idx)); bytes.Compare(d, lr.OldPayload) != 0 {
			return fmt.Errorf("payload is different than logged: %v", base64.StdEncoding.EncodeToString(d))
		}

		if err := sp.DeleteAt(int(lr.Idx)); err != nil {
			return err
		}
	case wal.TypeSet, wal.TypeFreePageSet:
		sp := convertSlotted(p)
		if d := sp.GetAt(int(lr.Idx)); bytes.Compare(d, lr.OldPayload) != 0 {
			return errors.New("payload is different than logged")
		}

		if err := sp.SetAt(int(lr.Idx), lr.Payload); err != nil {
			return err
		}
	case wal.TypeCopyAt:
		cp := convertCopyAt(p)
		if d := cp.ReadAt(lr.Idx, len(lr.Payload)); bytes.Compare(d, lr.OldPayload) != 0 {
			return errors.New("payload is different than logged")
		}

		cp.CopyAt(lr.Idx, lr.Payload)
	case wal.TypeFreePage:
		sp := convertSlotted(p)
		if d := sp.GetAt(int(lr.Idx)); bytes.Compare(d, lr.OldPayload) != 0 {
			return errors.New("payload is different than logged")
		}

		if err := sp.SetAt(int(lr.Idx), lr.Payload); err != nil {
			return err
		}
	case wal.TypeNewPage:
		sp := convertSlotted(p)
		if lr.IsPop() {
			if d := sp.GetAt(int(lr.Idx)); bytes.Compare(d, lr.OldPayload) != 0 {
				return errors.New("payload is different than logged")
			}

			if err := sp.SetAt(int(lr.Idx), lr.Payload); err != nil {
				return err
			}
		}
	default:
		panic("unknown log type")
	}

	// then update page lsn
	p.SetPageLSN(lr.Lsn)
	p.SetDirty()

	return nil
}

// Undo appends a clr entry for given log and undoes it. Note that undo is not a conditional operation unlike redo,
// meaning, there is no need to compare pageLsn etc. All log records are always undone unconditionally. This is
// because undo phase starts after redo phase completed and that means all log records are already known to be applied.
func (r *Recovery) Undo(txn transaction.Transaction, lr *wal.LogRecord) error {
	// 1. create clr
	// 2. append clr to wal
	// 3. fetch page from buffer pool and cast it to slotted page
	// 4. undo action taken by log
	if lr.Type() == wal.TypeCommit {
		return fmt.Errorf("undoing committed txn: %v", lr.TxnID)
	}

	if common.OneOf(lr.Type(), wal.TypeAbort, wal.TypeTxnStarter) {
		return nil
	}

	p, err := r.dm.GetPage(lr.PageID)
	if err != nil {
		return err
	}
	defer r.dm.Unpin(lr.PageID)

	common.Assert(p.GetPageLSN() >= lr.Lsn, "corrupted redo phase")

	switch lr.Type() {
	case wal.TypePageFormat:
		// do nothing, page format always comes after new page and this page will be collected by freelist
	case wal.TypeDelete:
		p := convertSlotted(p)
		common.PanicIfErr(p.InsertAt(int(lr.Idx), lr.OldPayload))

		// append clr
		lsn := r.logManager.AppendLog(txn, wal.NewInsertLogRecord(lr.TxnID, lr.Idx, lr.Payload, p.GetPageId()))
		p.SetPageLSN(lsn)
	case wal.TypeSet:
		p := convertSlotted(p)
		d := p.GetAt(int(lr.Idx))
		if bytes.Compare(d, lr.Payload) != 0 {
			return fmt.Errorf("payload is different than logged: %v", base64.StdEncoding.EncodeToString(d))
		}

		common.PanicIfErr(p.SetAt(int(lr.Idx), lr.OldPayload))

		// append clr
		lsn := r.logManager.AppendLog(txn, wal.NewSetLogRecord(lr.TxnID, lr.Idx, common.Clone(lr.OldPayload), d, lr.PageID))
		p.SetPageLSN(lsn)
	case wal.TypeInsert:
		p := convertSlotted(p)
		d := p.GetAt(int(lr.Idx))
		if bytes.Compare(d, lr.Payload) != 0 {
			panic("payload is different than logged")
		}

		common.PanicIfErr(p.DeleteAt(int(lr.Idx)))

		// append clr
		lsn := r.logManager.AppendLog(txn, wal.NewDeleteLogRecord(lr.TxnID, lr.Idx, d, lr.PageID))
		p.SetPageLSN(lsn)
	case wal.TypeCopyAt:
		p := convertCopyAt(p)
		d := p.ReadAt(lr.Idx, len(lr.Payload))
		if bytes.Compare(d, lr.Payload) != 0 {
			return fmt.Errorf("payload is different than logged: %v", base64.StdEncoding.EncodeToString(d))
		}

		p.CopyAt(lr.Idx, lr.OldPayload)

		// append clr
		lsn := r.logManager.AppendLog(txn, wal.NewCopyAtLogRecord(lr.TxnID, lr.Idx, common.Clone(lr.OldPayload), common.Clone(d), lr.PageID))
		p.SetPageLSN(lsn)
	case wal.TypeNewPage:
		if err := r.dm.FreePage(txn, lr.PageID); err != nil {
			return fmt.Errorf("failed to free page: %w", err)
		}
	default:
		return fmt.Errorf("unrecognized log record type encountered for undo, type: %v", lr.Type())
	}

	return nil
}

func convertSlotted(p *pages.RawPage) *pages.SlottedPage {
	sp := pages.CastSlottedPage(p)
	return &sp
}

func convertCopyAt(p *pages.RawPage) *pages.CopyAtPage {
	cp := pages.CastCopyAtPage(p)
	return &cp
}
