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
		t:          TypeInsert,
		txnID:      123,
		lsn:        12,
		prevLsn:    123,
		idx:        123,
		payload:    []byte("sa"),
		oldPayload: []byte("as"),
		pageID:     123,
		prevPageID: 123,
	}, buf)

	t.Log(buf.String())

	lr, _ := s.Deserialize(buf)
	t.Log(lr.payload)
}
