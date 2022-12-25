package wal

import (
	"helin/disk/pages"
)

type LogRecordType uint8
type TxnID uint64

const (
	TypeInvalid = iota
	TypeInsert
	TypeSet
	TypeDelete
	TypeNewPage
	TypeFreePage
)

const (
	LogRecordInlineSize = 1 + 8 + 8 + 8 + 2 + 8 + 8
)

type LogRecord struct {
	t       LogRecordType
	txnID   TxnID
	lsn     pages.LSN
	prevLsn pages.LSN

	// for delete, insert and set
	idx     uint16
	payload []byte

	// for update
	oldPayload []byte

	// for new page
	pageID     uint64
	prevPageID uint64
}

func NewInsertLogRecord(txnID TxnID, idx uint16, payload []byte) *LogRecord {
	return &LogRecord{t: TypeInsert, txnID: txnID, idx: idx, payload: payload}
}

func NewDeleteLogRecord(txnID TxnID, idx uint16, deleted []byte) *LogRecord {
	return &LogRecord{t: TypeDelete, txnID: txnID, idx: idx, oldPayload: deleted}
}

func NewSetLogRecord(txnID TxnID, idx uint16, payload, oldPayload []byte) *LogRecord {
	return &LogRecord{t: TypeSet, txnID: txnID, idx: idx, payload: payload, oldPayload: oldPayload}
}

func NewAllocPageLogRecord(txnID TxnID, pageID uint64) *LogRecord {
	return &LogRecord{t: TypeNewPage, txnID: txnID, pageID: pageID}
}

func NewFreePageLogRecord(txnID TxnID, pageID uint64) *LogRecord {
	return &LogRecord{t: TypeFreePage, txnID: txnID, pageID: pageID}
}
