package wal

import (
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
)

type LogRecordType uint8

const (
	TypeInvalid = iota
	TypeInsert
	TypeSet
	TypeDelete
	TypeCopyAt
	TypeNewPage
	TypeFreePage
	TypeFreePageSet
	TypePageFormat
	TypeCheckpointBegin
	TypeCheckpointEnd
	TypeTxnStarter
	TypeCommit
	TypeTxnEnd
	TypeAbort
)

type LogRecord struct {
	T          LogRecordType
	TxnID      transaction.TxnID
	FreedPages []uint64
	Lsn        pages.LSN
	PrevLsn    pages.LSN

	// for delete, insert and set
	Idx     uint16
	Payload []byte

	// for update
	OldPayload []byte

	// for free list logs
	PageID     uint64
	TailPageID uint64
	HeadPageID uint64

	// for page formats
	PageType uint64

	// indicates if this is a clr log record
	IsClr bool

	UndoNext pages.LSN
	Actives  []transaction.TxnID

	Raw []byte
}

func (l *LogRecord) Type() LogRecordType {
	return l.T
}

func (l *LogRecord) IsPop() bool {
	common.Assert(l.T == TypeNewPage, "IsPop called on a log type other than TypeNewPage")
	return len(l.Payload) > 0
}

func (l *LogRecord) GetTxnID() transaction.TxnID {
	if l.T == TypeTxnStarter {
		return transaction.TxnID(l.Lsn)
	}
	return l.TxnID
}

func NewInsertLogRecord(txnID transaction.TxnID, idx uint16, payload []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeInsert, TxnID: txnID, Idx: idx, Payload: payload, PageID: pageID}
}

func NewDeleteLogRecord(txnID transaction.TxnID, idx uint16, deleted []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeDelete, TxnID: txnID, Idx: idx, OldPayload: deleted, PageID: pageID}
}

func NewSetLogRecord(txnID transaction.TxnID, idx uint16, payload, oldPayload []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeSet, TxnID: txnID, Idx: idx, Payload: payload, OldPayload: oldPayload, PageID: pageID}
}

func NewCopyAtLogRecord(txnID transaction.TxnID, offset uint16, payload, oldPayload []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeCopyAt, TxnID: txnID, Idx: offset, Payload: payload, OldPayload: oldPayload, PageID: pageID}
}

func NewPageFormatLogRecord(txnID transaction.TxnID, pageType, pageID uint64) *LogRecord {
	return &LogRecord{T: TypePageFormat, TxnID: txnID, PageType: pageType, PageID: pageID}
}

func NewAllocPageLogRecord(txnID transaction.TxnID, idx uint16, payload, oldPayload []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeNewPage, TxnID: txnID, Idx: idx, Payload: payload, OldPayload: oldPayload, PageID: pageID}
}

func NewDiskAllocPageLogRecord(txnID transaction.TxnID, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeNewPage, TxnID: txnID, PageID: pageID}
}

func NewFreePageLogRecord(txnID transaction.TxnID, idx uint16, payload, oldPayload []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeFreePage, TxnID: txnID, Idx: idx, Payload: payload, OldPayload: oldPayload, PageID: pageID}
}

func NewFreePageSetLogRecord(txnID transaction.TxnID, idx uint16, payload, oldPayload []byte, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeFreePageSet, TxnID: txnID, Idx: idx, Payload: payload, OldPayload: oldPayload, PageID: pageID}
}

func NewAbortLogRecord(txnID transaction.TxnID) *LogRecord {
	return &LogRecord{T: TypeAbort, TxnID: txnID}
}

func NewCommitLogRecord(txnID transaction.TxnID, freed []uint64) *LogRecord {
	return &LogRecord{T: TypeCommit, TxnID: txnID, FreedPages: freed}
}

func NewTxnStarterLogRecord() *LogRecord {
	return &LogRecord{T: TypeTxnStarter}
}

func NewTxnEndLogRecord(txnID transaction.TxnID) *LogRecord {
	return &LogRecord{T: TypeTxnEnd, TxnID: txnID}
}

func NewCheckpointBeginLogRecord(activeTxnList ...transaction.TxnID) *LogRecord {
	return &LogRecord{T: TypeCheckpointBegin, Actives: activeTxnList}
}

func NewCheckpointEndLogRecord(startLSN pages.LSN, activeTxnList ...transaction.TxnID) *LogRecord {
	return &LogRecord{T: TypeCheckpointEnd, Actives: activeTxnList, PrevLsn: startLSN}
}
