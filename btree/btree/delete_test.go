package btree

import (
	"fmt"
	"helin/common"
	"helin/transaction"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type kv struct {
	k, v string
}

func TestDelete(t *testing.T) {
	t.Run("count should be zero after all is deleted", func(t *testing.T) {
		pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
		tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 100_000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 1000) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), StringKey(k), v)
			keys = append(keys, kv{k: k, v: v})
		}

		t.Logf("inserted %v keys", numKeys)

		assert.Equal(t, numKeys, tree.Count(transaction.TxnTODO()))

		for _, kv := range keys {
			ok := tree.Delete(transaction.TxnNoop(), StringKey(kv.k))
			assert.True(t, ok)
		}

		assert.Zero(t, tree.Count(transaction.TxnTODO()))
		assert.Equal(t, 1, tree.Height(transaction.TxnTODO()))
	})

	t.Run("count should be zero after all is deleted no overflow", func(t *testing.T) {
		pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
		tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 1_000_000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 32) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), StringKey(k), v)
			keys = append(keys, kv{k: k, v: v})
		}

		t.Logf("inserted %v keys", numKeys)

		assert.Equal(t, numKeys, tree.Count(transaction.TxnTODO()))

		for _, kv := range keys {
			ok := tree.Delete(transaction.TxnNoop(), StringKey(kv.k))
			assert.True(t, ok)
		}

		assert.Zero(t, tree.Count(transaction.TxnTODO()))
	})

	t.Run("other items should not be affected", func(t *testing.T) {
		pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
		tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 1000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 1000) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), StringKey(k), v)
			keys = append(keys, kv{k: k, v: v})
		}

		tree.Print(transaction.TxnTODO())

		for i, kv := range keys {
			ok := tree.Delete(transaction.TxnNoop(), StringKey(kv.k))
			assert.True(t, ok)

			for _, kv2 := range keys[i+1:] {
				v := tree.Get(transaction.TxnNoop(), StringKey(kv2.k))
				assert.EqualValues(t, v, kv2.v)
			}
		}
	})
}
