package wal

import (
	"bytes"
	"testing"
)

func TestDefaultLogRecordSerializer(t *testing.T) {
	s := &DefaultLogRecordSerializer{
		area: make([]byte, 0, 100),
	}

	buf := &bytes.Buffer{}
	s.Serialize(&LogRecord{
		T:          TypeInsert,
		TxnID:      123,
		Lsn:        12,
		PrevLsn:    123,
		Idx:        123,
		Payload:    []byte("sa"),
		OldPayload: []byte("as"),
		PageID:     123,
		PrevPageID: 123,
	}, buf)

	t.Log(buf.String())

	lr, _, _ := s.Deserialize(buf)
	t.Log(lr.Payload)
}
