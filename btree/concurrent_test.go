package btree

import (
	"fmt"
	"helin/buffer"
	"helin/common"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrent_Inserts(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 4096)
	tree := NewBtreeWithPager(50, NewBufferPoolPager(pool, &PersistentKeySerializer{}))
	log.SetOutput(io.Discard)

	rand.Seed(42)
	n, chunkSize := 100_000, 10_000 // there will be n/chunkSize parallel routines
	inserted := rand.Perm(n)
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(PersistentKey(i), SlotPointer{
					PageId:  int64(i),
					SlotIdx: int16(i),
				})
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	assert.Equal(t, len(inserted), tree.Count())
	// assert they are sorted
	vals := tree.FindSince(PersistentKey(1))
	prev := -1
	for _, v := range vals {
		require.Less(t, int64(prev), v.(SlotPointer).PageId)
		prev = int(v.(SlotPointer).PageId)
	}
}

func TestConcurrent_Inserts2(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 4096)
	tree := NewBtreeWithPager(50, NewBufferPoolPager(pool, &PersistentKeySerializer{}))
	log.SetOutput(io.Discard)

	rand.Seed(42)
	n, chunkSize := 100000, 25000 // there will be n/chunkSize parallel routines
	inserted := rand.Perm(n)
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(PersistentKey(i), SlotPointer{
					PageId:  int64(i),
					SlotIdx: int16(i),
				})
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()
	println(tree.Height())

}

func TestConcurrent_Deletes(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 100_000)
	tree := NewBtreeWithPager(10, NewBufferPoolPager(pool, &PersistentKeySerializer{}))
	log.SetOutput(io.Discard)

	rand.Seed(42)
	n, chunkSize := 100_000, 1000 // there will be n/chunkSize parallel routines
	inserted := rand.Perm(n)
	for _, i := range inserted {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}
	assert.Equal(t, len(inserted), tree.Count())
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted[:50_000], chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				if !tree.Delete(PersistentKey(i)) {
					t.Errorf("%v cannot be found", i)
				}
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	for _, v := range inserted[:50_000] {
		assert.Nil(t, tree.Find(PersistentKey(v)))

	}
	for _, v := range inserted[50_000:] {
		p := tree.Find(PersistentKey(v)).(SlotPointer)
		require.Equal(t, int64(v), p.PageId)
	}

	assert.Equal(t, 50_000, tree.Count())
}

func TestConcurrent_Inserts_With_MemPager(t *testing.T) {
	log.SetOutput(io.Discard)
	memPager := NewMemPager(&StringKeySerializer{Len: -1}, &StringValueSerializer{Len: -1})
	tree := NewBtreeWithPager(10, memPager)

	rand.Seed(42)
	// NOTE: on my m1 macbook air this test takes 1.1 second with 25000 chunk size and 1.6
	// seconds with 100_000 chunk size(4 thread vs 1 thread)
	n, chunkSize := 1_000_000, 100_000
	inserted := rand.Perm(n)
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("key_%v_val_%v", i, i))
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	assert.Equal(t, len(inserted), tree.Count())
	// assert they are sorted
	it := NewTreeIterator(nil, tree, tree.pager)
	var prev common.Key = StringKey("")
	for k, v := it.Next(); k != nil; k, v = it.Next() {
		require.Less(t, prev, k)
		key := string(k.(StringKey))

		i, err := strconv.Atoi(strings.TrimPrefix(key, "key_"))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("key_%v_val_%v", i, i), v)

		prev = k
	}
}

func TestConcurrent_Hammer(t *testing.T) {
	log.SetOutput(io.Discard)
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 4096)
	tree := NewBtreeWithPager(50, NewBufferPoolPagerWithValueSerializer(pool, &StringKeySerializer{Len: -1}, &StringValueSerializer{Len: -1}))

	toDeleteN := 10_000
	toDelete := rand.Perm(toDeleteN)
	for _, i := range toDelete {
		tree.Insert(StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("key_%v_val_%v", i, i))
	}

	rand.Seed(42)

	n, chunkSize := 500_000, 500_000
	inserted := rand.Perm(n)
	for i := 0; i < len(inserted); i++ {
		inserted[i] += toDeleteN
	}

	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("key_%v_val_%v", i, i))
			}
			wg.Done()
		}(chunk)
	}

	for _, chunk := range common.ChunksInt(toDelete, 1000) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				if !tree.Delete(StringKey(fmt.Sprintf("key_%v", i))) {
					t.Error("key not found")
				}
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	assert.Equal(t, len(inserted), tree.Count())

	// assert they are sorted
	it := NewTreeIterator(nil, tree, tree.pager)
	var prev common.Key = StringKey("")
	for k, v := it.Next(); k != nil; k, v = it.Next() {
		require.Less(t, prev, k)
		key := string(k.(StringKey))

		i, err := strconv.Atoi(strings.TrimPrefix(key, "key_"))
		require.NoError(t, err)
		require.Equal(t, fmt.Sprintf("key_%v_val_%v", i, i), v)

		prev = k
	}

	// assert not found
	for _, v := range toDelete {
		assert.Nil(t, tree.Find(StringKey(fmt.Sprintf("key_%v", v))))
	}
}

// go test -run FuzzConcurrentInserts ./btree -fuzz=Fuzz -fuzztime 10s
func FuzzConcurrent_Inserts(f *testing.F) {
	keys := []string{"Hello", "world", " ", "!12345"}
	for _, tc := range keys {
		f.Add(tc)
	}

	memPager := NewMemPager(&StringKeySerializer{Len: -1}, &StringValueSerializer{Len: -1})
	tree := NewBtreeWithPager(10, memPager)
	f.Fuzz(func(t *testing.T, key string) {
		if len(key) > 1000 || key == "" {
			// NOTE: with overflow pages should pass this test without this if condition too
			// NOTE: without overflow pages, it fails this test
			return
		}
		tree.InsertOrReplace(StringKey(key), fmt.Sprintf("val_%v", key))

		// assert they are sorted
		it := NewTreeIterator(nil, tree, tree.pager)
		var prev common.Key = StringKey("")
		i := 0
		for k, v := it.Next(); k != nil; k, v = it.Next() {
			require.Less(t, prev, k)
			key := string(k.(StringKey))

			require.Equal(t, fmt.Sprintf("val_%v", key), v)

			prev = k
			i++
		}
		it.Close()
	})
}

// go test -run FuzzConcurrentInserts ./btree -fuzz=Fuzz -fuzztime 10s
func TestConcurrent_Insertsasd(t *testing.T) {
	memPager := NewMemPager(&StringKeySerializer{Len: -1}, &StringValueSerializer{Len: -1})
	tree := NewBtreeWithPager(50, memPager)

	m := sync.Mutex{}
	wg := sync.WaitGroup{}
	inserted := make([]string, 0)

	for i := 0; i < 100; i++ {
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
	it := NewTreeIterator(nil, tree, tree.pager)
	var prev common.Key = StringKey("")
	i := 0
	for k, v := it.Next(); k != nil; k, v = it.Next() {
		require.Less(t, prev, k)
		key := string(k.(StringKey))

		require.Equal(t, fmt.Sprintf("val_%v", key), v)

		prev = k
		i++
	}
	it.Close()

	// assert inserted keys
	t.Logf("inserted %v keys", len(inserted))
	for _, k := range inserted {
		val := tree.Find(StringKey(k))
		require.Equal(t, fmt.Sprintf("val_%v", k), val)
	}
}

func writer(tree *BTree, n int) []string {
	res := make([]string, 0)
	for i := 0; i < n; i++ {
		k := common.RandStr(1, 1000)
		v := fmt.Sprintf("val_%v", k)
		tree.InsertOrReplace(StringKey(k), v)
		res = append(res, k)
	}

	return res
}

func deleter(tree *BTree, n int, keys []string) []string {
	if len(keys) == 0 {
		return nil
	}

	res := make([]string, 0)
	indexes := rand.Perm(len(keys))
	for i := 0; i < n; i++ {
		k := keys[indexes[i]]
		if tree.Delete(StringKey(k)) {
			res = append(res, k)
		}
	}

	return res
}
