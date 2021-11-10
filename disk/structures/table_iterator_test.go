package structures

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/buffer"
	"helin/disk/pages"
	"os"
	"strconv"
	"testing"
)

func TestTableIterator(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)
	firstPage, _ := pool.NewPage()
	pages.FormatAsSlottedPage(firstPage)
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
		}, "")

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	it := NewTableIterator("", &table)
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
