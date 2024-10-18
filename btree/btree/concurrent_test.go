package btree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/common"
	"helin/transaction"
	"io"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// go test -run FuzzConcurrentInserts ./btree -fuzz=Fuzz -fuzztime 10s
func FuzzConcurrent_Inserts(f *testing.F) {
	keys := []string{"Hello", "world", " ", "!12345"}
	for _, tc := range keys {
		f.Add(tc)
	}

	pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	f.Fuzz(func(t *testing.T, key string) {
		if len(key) > 1000 || key == "" {
			// NOTE: with overflow pages should pass this test without this if condition too
			// NOTE: without overflow pages, it fails this test
			return
		}
		tree.Set(transaction.TxnNoop(), StringKey(key), fmt.Sprintf("val_%v", key))

		// assert they are sorted
		it := NewTreeIterator(transaction.TxnNoop(), tree)
		var prev common.Key = StringKey("")
		i := 0
		for k, v := it.Next(); k != nil; k, v = it.Next() {
			require.Less(t, prev, k)
			key := string(k.(StringKey))

			require.Equal(t, fmt.Sprintf("val_%v", key), v)

			prev = k
			i++
		}

		require.NoError(t, it.Close())
	})
}

func TestConcurrent_Inserts_With_MemPager(t *testing.T) {
	log.SetOutput(io.Discard)
	pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	// TODO: deadlock in this test
	r := rand.New(rand.NewSource(42))
	// NOTE: on my m1 macbook air this test takes 1.1 second with 25000 chunk size and 1.6
	// seconds with 100_000 chunk size(4 thread vs 1 thread)
	n, chunkSize := 1_000_000, 100_000
	inserted := r.Perm(n)
	wg := &sync.WaitGroup{}
	for _, chunk := range common.ChunksInt(inserted, chunkSize) {
		wg.Add(1)
		go func(arr []int) {
			for _, i := range arr {
				tree.Insert(transaction.TxnNoop(), StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("key_%v_val_%v", i, i))
			}
			wg.Done()
		}(chunk)
	}
	wg.Wait()

	c, err := tree.Count(transaction.TxnTODO())
	assert.NoError(t, err)
	assert.Equal(t, len(inserted), c)

	// assert they are sorted
	it := NewTreeIterator(transaction.TxnNoop(), tree)
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
