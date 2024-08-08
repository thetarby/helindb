package btree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"helin/common"
	"helin/transaction"
	"strconv"
	"testing"
)

func TestInsert(t *testing.T) {
	t.Run("count should be n after all is inserted", func(t *testing.T) {
		pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
		tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		numKeys := 100_000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 1000) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), StringKey(k), v)
		}

		assert.Equal(t, numKeys, tree.Count())
	})

	t.Run("items should be found after all is inserted", func(t *testing.T) {
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

		for _, kv := range keys {
			v := tree.Get(StringKey(kv.k))
			assert.EqualValues(t, kv.v, v)
		}
	})

	t.Run("items should be found after all is inserted with overflow", func(t *testing.T) {
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

		for _, kv := range keys {
			v := tree.Get(StringKey(kv.k))
			assert.EqualValues(t, kv.v, v)
		}
	})
}

func TestInsert_Or_Replace_Should_Return_False_When_Key_Exists(t *testing.T) {
	pager2 := NewPager2(NewMemBPager(4096*2), &PersistentKeySerializer{}, &StringValueSerializer{})
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	for i := 0; i < 1000; i++ {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), strconv.Itoa(i))
	}

	isInserted := tree.Set(transaction.TxnNoop(), PersistentKey(500), "new_500")

	assert.False(t, isInserted)
}

func TestInsert_Or_Replace_Should_Replace_Value_When_Key_Exists(t *testing.T) {
	pager2 := NewPager2(NewMemBPager(4096*2), &PersistentKeySerializer{}, &StringValueSerializer{})
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	for i := 0; i < 1000; i++ {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), strconv.Itoa(i))
	}

	tree.Set(transaction.TxnNoop(), PersistentKey(500), "new_500")
	val := tree.Get(PersistentKey(500))

	assert.Contains(t, val.(string), "new_500")
}
