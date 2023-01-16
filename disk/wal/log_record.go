package wal

import (
	"errors"
	"helin/disk/pages"
	"helin/transaction"
)

type LogRecordType uint8

const (
	TypeInvalid = iota
	TypeInsert
	TypeSet
	TypeDelete
	TypeNewPage
	TypeFreePage
	TypeCheckpointBegin
	TypeCheckpointEnd
	TypeTxnBegin
	TypeCommit
	TypeAbort
)

const (
	LogRecordInlineSize = 1 + 8 + 8 + 8 + 2 + 8 + 8
)

type LogRecord struct {
	T       LogRecordType
	TxnID   transaction.TxnID
	Lsn     pages.LSN
	PrevLsn pages.LSN

	// for delete, insert and set
	Idx     uint16
	Payload []byte

	// for update
	OldPayload []byte

	// for new page
	PageID     uint64
	PrevPageID uint64

	// indicates if this is a clr log record
	IsClr bool
}

func (l *LogRecord) Type() LogRecordType {
	return l.T
}

func (l *LogRecord) GetTxnID() transaction.TxnID {
	return l.TxnID
}

func (l *LogRecord) Clr() (*LogRecord, error) {
	var clr *LogRecord
	switch l.T {
	case TypeDelete:
		clr = NewInsertLogRecord(l.TxnID, l.Idx, l.OldPayload, l.PageID)
	case TypeSet:
		clr = NewSetLogRecord(l.TxnID, l.Idx, l.OldPayload, l.Payload, l.PageID)
	case TypeInsert:
		clr = NewDeleteLogRecord(l.TxnID, l.Idx, l.Payload, l.PageID)
	default:
		return nil, errors.New("log record cannot be negated")
	}

	clr.IsClr = true
	return clr, nil
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

func NewAllocPageLogRecord(txnID transaction.TxnID, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeNewPage, TxnID: txnID, PageID: pageID}
}

func NewFreePageLogRecord(txnID transaction.TxnID, pageID uint64) *LogRecord {
	return &LogRecord{T: TypeFreePage, TxnID: txnID, PageID: pageID}
}

func NewAbortLogRecord(txnID transaction.TxnID) *LogRecord {
	return &LogRecord{T: TypeAbort, TxnID: txnID}
}

func NewCheckpointBeginLogRecord(activeTxnList ...transaction.TxnID) *LogRecord {
	panic("implement me")
}

func NewCheckpointEndLogRecord(activeTxnList ...transaction.TxnID) *LogRecord {
	panic("implement me")
}
