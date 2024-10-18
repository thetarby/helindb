package pbtree

import (
	"fmt"
	"helin/btree/btree"
	"helin/buffer"
	"helin/common"
	"helin/disk"
	"helin/disk/wal"
	"helin/transaction"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mkPoolTemp(t *testing.T, poolSize int, dmFsync bool) *buffer.PoolV2 {
	dir, err := os.MkdirTemp("./", "")
	if err != nil {
		panic(err)
	}

	dbName := filepath.Join(dir, uuid.New().String())

	dm, _, err := disk.NewDiskManager(dbName, dmFsync)
	require.NoError(t, err)

	pool := buffer.NewBufferPoolV2WithDM(true, poolSize, dm, wal.NoopLM, nil)

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	})

	return pool
}

func TestConcurrent_Inserts_No_Errors(t *testing.T) {
	pool := mkPoolTemp(t, 1024, false)
	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	log.SetOutput(io.Discard)

	r := rand.New(rand.NewSource(42))
	n, chunkSize := 100000, 15000 // there will be n/chunkSize parallel routines
	inserted := r.Perm(n)
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("val_%v", i))
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()
}

func TestConcurrent_Inserts_Keys_Are_Sorted(t *testing.T) {
	pool := mkPoolTemp(t, 4096, false)
	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.PersistentKeySerializer{}, &btree.SlotPointerValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	log.SetOutput(io.Discard)

	r := rand.New(rand.NewSource(42))
	n, chunkSize := 100_000, 10_000 // there will be n/chunkSize parallel routines
	inserted := r.Perm(n)
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(transaction.TxnNoop(), btree.PersistentKey(i), btree.SlotPointer{
					PageId:  uint64(i),
					SlotIdx: int16(i),
				})
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	assert.Equal(t, len(inserted), tree.Count(transaction.TxnTODO()))
	// assert they are sorted
	vals := tree.FindBetween(btree.PersistentKey(10), btree.PersistentKey(99999999), 9999999)
	prev := 9
	for _, v := range vals {
		require.Less(t, uint64(prev), v.(btree.SlotPointer).PageId)
		prev = int(v.(btree.SlotPointer).PageId)
	}
}

func TestConcurrent_Inserts_Keys_Can_Be_Found(t *testing.T) {
	pool := mkPoolTemp(t, 4096, false)
	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	m := sync.Mutex{}
	wg := sync.WaitGroup{}
	inserted := make([]string, 0)

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			written := writer(tree, 100)
			m.Lock()
			inserted = append(inserted, written...)
			m.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	// assert they are sorted
	it := btree.NewTreeIterator(transaction.TxnNoop(), tree)
	var prev common.Key = btree.StringKey("")
	i := 0
	for k, v := it.Next(); k != nil; k, v = it.Next() {
		require.Less(t, prev, k)
		key := string(k.(btree.StringKey))

		require.Equal(t, fmt.Sprintf("val_%v", key), v)

		prev = k
		i++
	}
	require.NoError(t, it.Close())

	// assert inserted keys
	t.Logf("inserted %v keys", len(inserted))
	for _, k := range inserted {
		val := tree.Get(transaction.TxnNoop(), btree.StringKey(k))
		require.Equal(t, fmt.Sprintf("val_%v", k), val)
	}
}

func TestConcurrent_Inserts_Keys_Can_Be_Found_In_Mem(t *testing.T) {
	pager2 := btree.NewPager2(btree.NewMemBPager(4096*2), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	m := sync.Mutex{}
	wg := sync.WaitGroup{}
	inserted := make([]string, 0)

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			written := writer(tree, 1000)
			m.Lock()
			inserted = append(inserted, written...)
			m.Unlock()
			wg.Done()
		}()
	}

	wg.Wait()

	// assert they are sorted
	it := btree.NewTreeIterator(transaction.TxnNoop(), tree)
	var prev common.Key = btree.StringKey("")
	i := 0
	for k, v := it.Next(); k != nil; k, v = it.Next() {
		require.Less(t, prev, k)
		key := string(k.(btree.StringKey))

		require.Equal(t, fmt.Sprintf("val_%v", key), v)

		prev = k
		i++
	}
	require.NoError(t, it.Close())

	// assert inserted keys
	t.Logf("inserted %v keys", len(inserted))
	for _, k := range inserted {
		val := tree.Get(transaction.TxnNoop(), btree.StringKey(k))
		require.Equal(t, fmt.Sprintf("val_%v", k), val)
	}
}

func TestConcurrent_Deletes(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run("concurrent deletes", func(t *testing.T) {
			pool := mkPoolTemp(t, 100_000, false)
			pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.PersistentKeySerializer{}, &btree.SlotPointerValueSerializer{})
			tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

			r := rand.New(rand.NewSource(42))
			n, chunkSize := 100_000, 1000 // there will be n/chunkSize parallel routines
			inserted := r.Perm(n)
			for _, i := range inserted {
				tree.Insert(transaction.TxnNoop(), btree.PersistentKey(i), btree.SlotPointer{
					PageId:  uint64(i),
					SlotIdx: int16(i),
				})
			}

			t.Log("inserted")

			assert.Equal(t, len(inserted), tree.Count(transaction.TxnTODO()))

			wg := &sync.WaitGroup{}
			for _, chunk := range common.ChunksInt(inserted[:50_000], chunkSize) {
				wg.Add(1)
				go func(arr []int) {
					for _, i := range arr {
						if !tree.Delete(transaction.TxnNoop(), btree.PersistentKey(i)) {
							t.Errorf("%v cannot be found", i)
						}
					}
					wg.Done()
				}(chunk)
			}
			wg.Wait()

			t.Log("validating")
			for _, v := range inserted[:50_000] {
				assert.Nil(t, tree.Get(transaction.TxnNoop(), btree.PersistentKey(v)))

			}
			for _, v := range inserted[50_000:] {
				p := tree.Get(transaction.TxnNoop(), btree.PersistentKey(v)).(btree.SlotPointer)
				require.Equal(t, uint64(v), p.PageId)
			}

			assert.Equal(t, 50_000, tree.Count(transaction.TxnTODO()))
		})
	}
}

func TestConcurrent_Hammer(t *testing.T) {
	for i := 0; i < 10; i++ {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			pool := mkPoolTemp(t, 4096, false)
			pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
			tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

			// first insert some items later to be deleted
			toDeleteN := 100_000
			toDelete := rand.Perm(toDeleteN)
			for _, i := range toDelete {
				tree.Insert(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("key_%v_val_%v", i, i))
			}

			t.Log("populated tree")

			rand.Seed(42)

			// now generate items that will be inserted in parallel while other goroutines will delete previously inserted
			// items again in parallel.
			n, chunkSize := 500_000, 50_000
			toInsert := rand.Perm(n)
			for i := 0; i < len(toInsert); i++ {
				toInsert[i] += toDeleteN
			}

			wg := &sync.WaitGroup{}

			// initiate insert routines
			for _, chunk := range common.ChunksInt(toInsert, chunkSize) {
				wg.Add(1)
				go func(arr []int) {
					for _, i := range arr {
						tree.Insert(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("key_%v_val_%v", i, i))
					}
					wg.Done()
				}(chunk)
			}

			// initiate delete routines
			for _, chunk := range common.ChunksInt(toDelete, 1000) {
				wg.Add(1)
				go func(arr []int) {
					for _, i := range arr {
						key := fmt.Sprintf("key_%v", i)
						if !tree.Delete(transaction.TxnNoop(), btree.StringKey(key)) {
							t.Errorf("key not found: %v", key)
							tree.Delete(transaction.TxnNoop(), btree.StringKey(key))
						}
					}
					wg.Done()
				}(chunk)
			}
			wg.Wait()

			t.Log("validating")

			assert.Equal(t, len(toInsert), tree.Count(transaction.TxnTODO()))

			// assert they are sorted
			it := btree.NewTreeIterator(transaction.TxnNoop(), tree)
			var prev common.Key = btree.StringKey("")
			for k, v := it.Next(); k != nil; k, v = it.Next() {
				require.Less(t, prev, k)
				key := string(k.(btree.StringKey))

				i, err := strconv.Atoi(strings.TrimPrefix(key, "key_"))
				require.NoError(t, err)
				require.Equal(t, fmt.Sprintf("key_%v_val_%v", i, i), v)

				prev = k
			}

			// assert not found
			for _, v := range toDelete {
				assert.Nil(t, tree.Get(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%v", v))))
			}

			// assert found
			for _, v := range toInsert {
				key, val := fmt.Sprintf("key_%v", v), fmt.Sprintf("key_%v_val_%v", v, v)
				gotVal := tree.Get(transaction.TxnNoop(), btree.StringKey(key))
				assert.Equal(t, val, gotVal)
			}
		})
	}
}

func writer(tree *btree.BTree, n int) []string {
	res := make([]string, 0)
	for i := 0; i < n; i++ {
		k := common.RandStr(1, 1000)
		v := fmt.Sprintf("val_%v", k)
		tree.Set(transaction.TxnNoop(), btree.StringKey(k), v)
		res = append(res, k)
	}

	return res
}
