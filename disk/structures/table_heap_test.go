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

func TestTableHeap(t *testing.T) {
	dbName := uuid.New().String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 2)
	firstPage, _ := pool.NewPage(transaction.TxnTODO())
	pages.InitHeapPage(firstPage)
	table := TableHeap{
		Pool:        pool,
		FirstPageID: firstPage.GetPageId(),
		LastPageID:  0,
	}

	rid, err := table.InsertTuple(Row{
		Data: make([]byte, 10),
		Rid:  Rid{},
	}, transaction.TxnNoop())

	assert.NoError(t, err)
	assert.Equal(t, firstPage.GetPageId(), rid.PageId)
}

func TestTableHeap_All_Inserted_Should_Be_Found_And_Not_Inserted_Should_Not_Be_Found(t *testing.T) {
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

	inserted := make([]Rid, 0)
	for i := 0; i < 3000; i++ {
		rid, err := table.InsertTuple(Row{
			Data: []byte(strconv.Itoa(i)),
			Rid:  Rid{},
		}, transaction.TxnNoop())

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	for i := 0; i < 3000; i++ {
		rid := inserted[i]
		tuple := Row{}
		table.ReadTuple(rid, &tuple, transaction.TxnNoop())

		assert.Equal(t, []byte(strconv.Itoa(i)), tuple.Data)
	}
}

func TestTableHeap_Delete(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)
	log.SetOutput(io.Discard)

	pool := buffer.NewBufferPool(dbName, 32)
	firstPage, _ := pool.NewPage(transaction.TxnTODO())
	pages.InitHeapPage(firstPage)
	table := TableHeap{
		Pool:        pool,
		FirstPageID: firstPage.GetPageId(),
		LastPageID:  0,
	}

	inserted := make([]Rid, 0)
	for i := 0; i < 10_000; i++ {
		rid, err := table.InsertTuple(Row{
			Data: []byte(strconv.Itoa(i)),
			Rid:  Rid{},
		}, transaction.TxnNoop())

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	toDelete := []int{5, 8, 9}
	for _, i := range toDelete {
		err := table.HardDeleteTuple(inserted[i], transaction.TxnNoop())
		assert.NoError(t, err)
	}

	for i := 0; i < 10_000; i++ {
		rid := inserted[i]
		tuple := Row{}
		if common.Contains(toDelete, i) {
			table.ReadTuple(rid, &tuple, transaction.TxnNoop())
			require.Nil(t, tuple.GetData())
			continue
		}
		table.ReadTuple(rid, &tuple, transaction.TxnNoop())

		require.Equal(t, []byte(strconv.Itoa(i)), tuple.Data)
	}
}

func TestTableHeap_Delete_Last_Inserted_Item(t *testing.T) {
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

	inserted := make([]Rid, 0)
	for i := 0; i < 10; i++ {
		rid, err := table.InsertTuple(Row{
			Data: []byte(strconv.Itoa(i)),
			Rid:  Rid{},
		}, transaction.TxnNoop())

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	toDelete := []int{9}
	for _, i := range toDelete {
		err := table.HardDeleteTuple(inserted[i], transaction.TxnNoop())
		assert.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		if common.Contains(toDelete, i) {
			continue
		}

		rid := inserted[i]
		tuple := Row{}
		table.ReadTuple(rid, &tuple, transaction.TxnNoop())

		require.Equal(t, []byte(strconv.Itoa(i)), tuple.Data)
	}
}

func TestTableHeap_Delete_First_Inserted_Item(t *testing.T) {
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

	inserted := make([]Rid, 0)
	for i := 0; i < 10; i++ {
		rid, err := table.InsertTuple(Row{
			Data: []byte(strconv.Itoa(i)),
			Rid:  Rid{},
		}, transaction.TxnNoop())

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	toDelete := []int{0}
	for _, i := range toDelete {
		err := table.HardDeleteTuple(inserted[i], transaction.TxnNoop())
		assert.NoError(t, err)
	}

	for i := 0; i < 10; i++ {
		if common.Contains(toDelete, i) {
			continue
		}

		rid := inserted[i]
		tuple := Row{}
		table.ReadTuple(rid, &tuple, transaction.TxnNoop())

		require.Equal(t, []byte(strconv.Itoa(i)), tuple.Data)
	}
}

func TestTableHeap_Update(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)
	log.SetOutput(io.Discard)

	pool := buffer.NewBufferPool(dbName, 32)
	firstPage, _ := pool.NewPage(transaction.TxnTODO())
	pages.InitHeapPage(firstPage)
	table := TableHeap{
		Pool:        pool,
		FirstPageID: firstPage.GetPageId(),
		LastPageID:  0,
	}

	inserted := make([]Rid, 0)
	for i := 0; i < 100; i++ {
		rid, err := table.InsertTuple(Row{
			Data: []byte(strconv.Itoa(i)),
			Rid:  Rid{},
		}, transaction.TxnNoop())

		assert.NoError(t, err)
		inserted = append(inserted, rid)
	}

	toUpdate := []int{15, 25, 35}
	for _, i := range toUpdate {
		err := table.UpdateTuple(Row{Data: []byte("updated")}, inserted[i], transaction.TxnNoop())
		assert.NoError(t, err)
	}

	for i := 0; i < 100; i++ {
		rid := inserted[i]
		tuple := Row{}
		if common.Contains(toUpdate, i) {
			table.ReadTuple(rid, &tuple, transaction.TxnNoop())
			require.Equal(t, []byte("updated"), tuple.GetData())
			continue
		}
		table.ReadTuple(rid, &tuple, transaction.TxnNoop())

		require.Equal(t, []byte(strconv.Itoa(i)), tuple.Data)
	}
}
