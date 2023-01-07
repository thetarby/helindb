package structures

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/buffer"
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
	"io"
	"log"
	"strconv"
	"testing"
)

func TestTableIterator(t *testing.T) {
	log.SetOutput(io.Discard)
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)
	firstPage, _ := pool.NewPage(transaction.TxnTODO())
	pages.InitHeapPage(firstPage)
	table := TableHeap{
		Pool:        pool,
		FirstPageID: firstPage.GetPageId(),
		LastPageID:  0,
	}
	n := 3000
	inserted := make([]Rid, 0)
	for i := 0; i < n; i++ {
		rid, err := table.InsertTuple(Row{
			Data: []byte(strconv.Itoa(i)),
			Rid:  Rid{},
		}, transaction.TxnNoop())

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	it := NewTableIterator(transaction.TxnNoop(), &table)
	i := 0
	for {
		n := it.Next()
		if n == nil {
			break
		}

		require.Equal(t, strconv.Itoa(i), string(n.Data))
		i++
	}

	assert.Equal(t, n, i)
}
