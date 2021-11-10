package structures

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"helin/buffer"
	"helin/disk/pages"
	"os"
	"strconv"
	"testing"
)

func TestTableHeap(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 2)
	firstPage, _ := pool.NewPage()
	pages.FormatAsSlottedPage(firstPage)
	table := TableHeap{
		pool:        pool,
		firstPageID: firstPage.GetPageId(),
		lastPageID:  0,
	}

	rid, err := table.InsertTuple(Tuple{
		data: make([]byte, 10),
		rid:  Rid{},
	}, "")

	assert.NoError(t, err)
	assert.Equal(t, firstPage.GetPageId(), int(rid.PageId))
}

func TestTableHeap_All_Inserted_Should_Be_Found_And_Not_Inserted_Should_Not_Be_Found(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)
	firstPage, _ := pool.NewPage()
	pages.FormatAsSlottedPage(firstPage)
	table := TableHeap{
		pool:        pool,
		firstPageID: firstPage.GetPageId(),
		lastPageID:  0,
	}

	inserted := make([]Rid, 0)
	for i := 0; i < 3000; i++ {
		rid, err := table.InsertTuple(Tuple{
			data: []byte(strconv.Itoa(i)),
			rid:  Rid{},
		}, "")

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	for i := 0; i < 3000; i++ {
		rid := inserted[i]
		tuple := Tuple{}
		table.ReadTuple(rid, &tuple, "")

		assert.Equal(t, []byte(strconv.Itoa(i)), tuple.data)
	}
}
