package wal

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDefaultLogRecordSerializer(t *testing.T) {
	s := &DefaultLogRecordSerializer{
		area: make([]byte, 0, 100),
	}

	buf := &bytes.Buffer{}
	gw := NewGroupWriter(100, buf)
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
	}, gw)

	require.NoError(t, gw.SwapAndWaitFlush())

	lr, _, _ := s.Deserialize(buf)
	t.Log(string(lr.Payload))
	require.Equal(t, "sa", string(lr.Payload))
	require.Equal(t, "as", string(lr.OldPayload))
}
