package db

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"helin/btree/btree"
	"helin/common"
	"helin/transaction"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
)

type kv struct {
	k, v string
}

func mkDBTemp(t *testing.T, poolSize int, dmFsync bool) *DB {
	dir, err := os.MkdirTemp("./", "")
	if err != nil {
		panic(err)
	}

	dbName := filepath.Join(dir, uuid.New().String())

	db := OpenDB(dbName, poolSize, dmFsync)
	db.StartCheckpointRoutine()

	return db
}

func TestConcurrent_Hammer_Heap(t *testing.T) {
	db := mkDBTemp(t, 4096, false)
	tm := db.Tm

	txn := tm.Begin()
	if err := db.CreateStore(txn, "test"); err != nil {
		panic(err)
	}

	tree := db.GetStore(txn, "test")
	if err := tm.Commit(txn); err != nil {
		panic(err)
	}

	// first insert some items later to be deleted
	toDeleteN, toInsertN, chunkSize := 10000, 50000, 5_000

	var items []kv
	for i := 0; i < toDeleteN+toInsertN; i++ {
		k := uniqueRandStr(50, 8000) + "__" + strconv.Itoa(i) + "__key"
		v := fmt.Sprintf("val_%v", k)

		items = append(items, kv{k: k, v: v})
	}

	for _, i := range items[:toDeleteN] {
		txn := tm.Begin()
		tree.Set(txn, btree.StringKey(i.k), i.v)
		if err := tm.Commit(txn); err != nil {
			panic(err)
		}
	}

	t.Log("populated tree")

	wg := &sync.WaitGroup{}

	// initiate insert routines
	for _, chunk := range common.Chunks(items[toDeleteN:], chunkSize) {
		wg.Add(1)
		go func(arr []kv) {
			for _, i := range arr {
				txn := tm.Begin()
				tree.Set(txn, btree.StringKey(i.k), i.v)
				if err := tm.Commit(txn); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(chunk)
	}

	// initiate delete routines
	for _, chunk := range common.Chunks(items[:toDeleteN], 1000) {
		wg.Add(1)
		go func(arr []kv) {
			for _, i := range arr {
				txn := tm.Begin()
				if !tree.Delete(txn, btree.StringKey(i.k)) {
					t.Errorf("key not found: %v", i.k)
				}
				if err := tm.Commit(txn); err != nil {
					panic(err)
				}
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	t.Log("validating")

	assert.Equal(t, toInsertN, tree.Count(transaction.TxnTODO()))

	// assert not found
	for _, i := range items[:toDeleteN] {
		assert.Nil(t, tree.Get(transaction.TxnNoop(), btree.StringKey(i.k)))
	}

	// assert found
	for _, i := range items[toDeleteN:] {
		gotVal := tree.Get(transaction.TxnNoop(), btree.StringKey(i.k))
		assert.Equal(t, i.v, gotVal)
	}
}

var lookup = map[string]bool{}

func uniqueRandStr(min, max uint) string {
	for {
		s := common.RandStr(min, max)
		if !lookup[s] {
			lookup[s] = true
			return s
		}
	}
}
