package wal

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
	"time"
)

func TestGroupWriter_Write(t *testing.T) {
	buf := bytes.Buffer{}
	gw := NewGroupWriter(100, &buf)
	gw.RunFlusher()

	assertBuf := bytes.Buffer{}
	w := io.MultiWriter(gw, &assertBuf)
	for i := 0; i < 100; i++ {
		_, err := w.Write([]byte(fmt.Sprintf("selam_%v", i)))
		if i%10 == 0 {
			time.Sleep(time.Millisecond * 500)
		}
		require.NoError(t, err)
	}

	require.NoError(t, gw.StopFlusher())
	t.Log(buf.String())
	assert.Equal(t, assertBuf.String(), buf.String())
}
