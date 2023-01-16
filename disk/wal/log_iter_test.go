package wal

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestLogIter_Curr(t *testing.T) {
	f, err := os.Open("test3.log")
	require.NoError(t, err)

	iter, err := NewLogIter(f, &DefaultLogRecordSerializer{area: make([]byte, 0, 100)})
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		r, err := iter.Next()
		require.NoError(t, err)
		b, _ := json.Marshal(r)
		t.Log(string(b))
	}

	t.Log("curr")
	for i := 0; i < 10; i++ {
		r, err := iter.Curr()
		require.NoError(t, err)
		b, _ := json.Marshal(r)
		t.Log(string(b))
	}

	t.Log("prev")
	for i := 0; i < 9; i++ {
		r, err := iter.Prev()
		require.NoError(t, err)
		b, _ := json.Marshal(r)
		t.Log(string(b))
	}

	_, err = iter.Prev()
	require.Error(t, err)

	t.Log("next")
	for i := 0; i < 100; i++ {
		r, err := iter.Next()
		require.NoError(t, err)
		b, _ := json.Marshal(r)
		t.Log(string(b))
	}
}
