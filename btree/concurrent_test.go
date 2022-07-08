package btree

import (
	"helin/buffer"
	"helin/common"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
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
	log.SetOutput(ioutil.Discard)

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
	log.SetOutput(ioutil.Discard)

	rand.Seed(42)
	n, chunkSize := 100_000, 100_000 // there will be n/chunkSize parallel routines
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
	tree := NewBtreeWithPager(50, NewBufferPoolPager(pool, &PersistentKeySerializer{}))
	log.SetOutput(ioutil.Discard)

	rand.Seed(42)
	n, chunkSize := 100_000, 5_000 // there will be n/chunkSize parallel routines
	inserted := rand.Perm(n)
	for _, i := range inserted {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}
	assert.Equal(t, len(inserted), tree.Count())
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
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

	for _, v := range inserted {
		assert.Nil(t, tree.Find(PersistentKey(v)))

	}

	assert.Zero(t, tree.Count())
}

func TestConcurrent_Inserts_With_MemPager(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	memPager := NewMemPager(&PersistentKeySerializer{}, nil)
	tree := NewBtreeWithPager(10, memPager)

	rand.Seed(42)
	// NOTE: on my m1 macbook air this test takes 1.1 second with 25000 chunk size and 1.6 
	// seconds with 100_000 chunk size(4 thread vs 1 thread)
	n, chunkSize := 100_000, 25_000 
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