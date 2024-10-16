package pheap

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/buffer"
	"helin/disk"
	"helin/disk/wal"
	"helin/heap/heap"
	"helin/transaction"
	"os"
	"path/filepath"
	"testing"
)

func mkPoolTemp(t *testing.T) buffer.Pool {
	dir, err := os.MkdirTemp("./", "")
	if err != nil {
		panic(err)
	}

	dbName := filepath.Join(dir, uuid.New().String())

	dm, _, err := disk.NewDiskManager(dbName, false)
	require.NoError(t, err)

	pool := buffer.NewBufferPoolV2WithDM(true, 128, dm, wal.NoopLM, nil)

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	})

	return pool
}

func TestPHeap(t *testing.T) {
	pager := NewBufferPoolPager(mkPoolTemp(t), wal.NoopLM)

	h, err := heap.InitHeap(transaction.TxnNoop(), 20, 4096*2, 4096, pager)
	assert.NoError(t, err)

	x := make([]byte, 6000)
	x = append(x, []byte("sa")...)

	for i := 0; i < 10; i++ {
		if err := h.SetAt(transaction.TxnNoop(), 0, x); err != nil {
			panic(err)
		}
	}

	if err := h.FreeEmptyPages(transaction.TxnNoop()); err != nil {
		panic(err)
	}

	b, err := h.GetAt(transaction.TxnNoop(), 0)
	assert.NoError(t, err)

	c, err := h.Count(transaction.TxnNoop())
	assert.NoError(t, err)

	assert.Equal(t, 1, c)
	assert.Equal(t, x, b)
}
