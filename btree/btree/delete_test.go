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

		c, err := tree.Count(transaction.TxnTODO())
		assert.NoError(t, err)
		assert.Equal(t, numKeys, c)

		for _, kv := range keys {
			ok, err := tree.Delete(transaction.TxnNoop(), StringKey(kv.k))
			assert.NoError(t, err)
			assert.True(t, ok)
		}

		count, err := tree.Count(transaction.TxnTODO())
		assert.NoError(t, err)
		assert.Zero(t, count)

		h, err := tree.Height(transaction.TxnTODO())
		assert.NoError(t, err)
		assert.Equal(t, 1, h)
	})

	t.Run("count should be zero after all is deleted no overflow", func(t *testing.T) {
		pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
		tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 1_000_000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 32) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			assert.NoError(t, tree.Insert(transaction.TxnNoop(), StringKey(k), v))
			keys = append(keys, kv{k: k, v: v})
		}

		t.Logf("inserted %v keys", numKeys)

		count, err := tree.Count(transaction.TxnTODO())
		assert.NoError(t, err)

		assert.Equal(t, numKeys, count)

		for _, kv := range keys {
			ok, err := tree.Delete(transaction.TxnNoop(), StringKey(kv.k))
			assert.NoError(t, err)
			assert.True(t, ok)
		}

		count, err = tree.Count(transaction.TxnTODO())
		assert.NoError(t, err)

		assert.Zero(t, count)
	})

	t.Run("other items should not be affected", func(t *testing.T) {
		pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
		tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 1000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 1000) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			assert.NoError(t, tree.Insert(transaction.TxnNoop(), StringKey(k), v))
			keys = append(keys, kv{k: k, v: v})
		}

		tree.Print(transaction.TxnTODO())

		for i, kv := range keys {
			ok, err := tree.Delete(transaction.TxnNoop(), StringKey(kv.k))
			assert.NoError(t, err)
			assert.True(t, ok)

			for _, kv2 := range keys[i+1:] {
				v, err := tree.Get(transaction.TxnNoop(), StringKey(kv2.k))
				assert.NoError(t, err)
				assert.EqualValues(t, v, kv2.v)
			}
		}
	})
}
