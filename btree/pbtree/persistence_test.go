package pbtree

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/btree/btree"
	"helin/buffer"
	"helin/common"
	"helin/disk"
	"helin/disk/wal"
	"helin/transaction"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

type kv struct {
	k, v string
}

func TestPersistent_Resources_Are_Released(t *testing.T) {
	pool := mkPoolTemp(t, 4096, false)
	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	n := 100_000
	items := rand.Perm(n)
	for _, i := range items {
		tree.Insert(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%06d", i)), fmt.Sprintf("val_%06d", i))
		assert.Zero(t, pool.Replacer.NumPinnedPages())
	}

	// test count
	require.Equal(t, n, tree.Count(transaction.TxnTODO()))
	assert.Zero(t, pool.Replacer.NumPinnedPages())

	// test iterator
	it := btree.NewTreeIterator(transaction.TxnNoop(), tree)
	for k, _ := it.Next(); k != nil; k, _ = it.Next() {
	}
	assert.Zero(t, pool.Replacer.NumPinnedPages())

	// test find
	rand.Shuffle(len(items), func(i, j int) { items[i], items[j] = items[j], items[i] })
	for _, i := range items {
		val := tree.Get(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%06d", i))).(string)
		require.Equal(t, fmt.Sprintf("val_%06d", i), val)
		require.Zero(t, pool.Replacer.NumPinnedPages())
	}

	// test insert or replace
	for _, i := range items[:10000] {
		val := tree.Set(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%06d", i)), fmt.Sprintf("val_replaced_%06d", i))
		require.False(t, val)
		require.Zero(t, pool.Replacer.NumPinnedPages())
	}

	for i, item := range items[:10000] {
		key, val := fmt.Sprintf("key_%06d", item), fmt.Sprintf("val_%06d", item)
		if i < 10000 {
			val = fmt.Sprintf("val_replaced_%06d", item)
		}

		found := tree.Get(transaction.TxnNoop(), btree.StringKey(key)).(string)
		require.Equal(t, found, val)
		require.Zero(t, pool.Replacer.NumPinnedPages())
	}

	// test delete
	for _, i := range items {
		val := tree.Delete(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("key_%06d", i)))
		require.True(t, val)
		require.Zero(t, pool.Replacer.NumPinnedPages())
	}
}

func TestPersistent_Delete(t *testing.T) {
	t.Run("count should be zero after all is deleted", func(t *testing.T) {
		t.Parallel()
		pool := mkPoolTemp(t, 4096, false)
		pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 100_000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 1000) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), btree.StringKey(k), v)
			keys = append(keys, kv{k: k, v: v})
		}

		t.Logf("inserted %v keys", numKeys)

		assert.Equal(t, numKeys, tree.Count(transaction.TxnTODO()))

		for _, kv := range keys {
			ok := tree.Delete(transaction.TxnNoop(), btree.StringKey(kv.k))
			assert.True(t, ok)
		}

		assert.Zero(t, tree.Count(transaction.TxnTODO()))
		assert.Equal(t, 1, tree.Height(transaction.TxnTODO()))
	})

	t.Run("count should be zero after all is deleted no overflow", func(t *testing.T) {
		t.Parallel()
		pool := mkPoolTemp(t, 4096, false)
		pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 1_000_000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 32) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), btree.StringKey(k), v)
			keys = append(keys, kv{k: k, v: v})
		}

		t.Logf("inserted %v keys", numKeys)

		assert.Equal(t, numKeys, tree.Count(transaction.TxnTODO()))

		for _, kv := range keys {
			ok := tree.Delete(transaction.TxnNoop(), btree.StringKey(kv.k))
			assert.True(t, ok)
		}

		assert.Zero(t, tree.Count(transaction.TxnTODO()))
	})

	t.Run("other items should not be affected", func(t *testing.T) {
		t.Parallel()
		pool := mkPoolTemp(t, 4096, false)
		pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

		keys := make([]kv, 0)
		numKeys := 1000
		for i := 0; i < numKeys; i++ {
			k := common.RandStr(1, 1000) + "__" + strconv.Itoa(i)
			v := fmt.Sprintf("val_%v", k)

			tree.Insert(transaction.TxnNoop(), btree.StringKey(k), v)
			keys = append(keys, kv{k: k, v: v})
		}

		tree.Print(transaction.TxnTODO())

		for i, kv := range keys {
			ok := tree.Delete(transaction.TxnNoop(), btree.StringKey(kv.k))
			assert.True(t, ok)

			for _, kv2 := range keys[i+1:] {
				v := tree.Get(transaction.TxnNoop(), btree.StringKey(kv2.k))
				assert.EqualValues(t, v, kv2.v)
			}
		}
	})
}

func TestPersistent_All_Inserted_Should_Be_Found_After_File_Is_Closed_And_Reopened(t *testing.T) {
	dir, err := os.MkdirTemp("./", "")
	if err != nil {
		panic(err)
	}

	dbName := filepath.Join(dir, uuid.New().String())

	dm, _, err := disk.NewDiskManager(dbName, false)
	require.NoError(t, err)

	pool := buffer.NewBufferPoolV2WithDM(true, 64, dm, wal.NoopLM, nil)

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	})

	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	keys := make([]kv, 0)
	numKeys := 10_000
	for i := 0; i < numKeys; i++ {
		k := common.RandStr(1, 32) + "__" + strconv.Itoa(i)
		v := fmt.Sprintf("val_%v", k)

		tree.Insert(transaction.TxnNoop(), btree.StringKey(k), v)
		keys = append(keys, kv{k: k, v: v})
	}

	require.NoError(t, pool.FlushAll())
	require.NoError(t, dm.Close())

	dm, _, err = disk.NewDiskManager(dbName, false)
	require.NoError(t, err)

	pool = buffer.NewBufferPoolV2WithDM(true, 64, dm, wal.NoopLM, nil)

	pager2 = btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	newTreeReference := btree.ConstructBtreeByMeta(transaction.TxnNoop(), tree.GetMetaPID(), pager2)

	rand.Shuffle(len(keys), func(i, j int) {
		t := keys[i]
		keys[i] = keys[j]
		keys[j] = t
	})
	for _, kv := range keys {
		val := newTreeReference.Get(transaction.TxnNoop(), btree.StringKey(kv.k))
		assert.EqualValues(t, kv.v, val)
	}

}

func TestPersistent_TreeIterator_Should_Return_Every_Value_Bigger_Than_Or_Equal_To_Key_When_Initialized_With_A_Key(t *testing.T) {
	pool := mkPoolTemp(t, 32, false)
	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
		assert.Zero(t, pool.Replacer.NumPinnedPages())
	}

	it := btree.NewTreeIteratorWithKey(transaction.TxnNoop(), btree.StringKey("selam_099"), tree)
	i := 9900
	for _, val := it.Next(); val != nil; _, val = it.Next() {
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
		i++
	}

	// all pages should be unpinned after process is finished
	assert.Zero(t, pool.Replacer.NumPinnedPages())
}

func TestPersistent_TreeIterator_Should_Return_All_Values_When_Initialized_Without_A_Key(t *testing.T) {
	pool := mkPoolTemp(t, 32, false)
	pager2 := btree.NewPager2(NewBufferPoolBPager(pool, wal.NoopLM), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), btree.StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
	}

	it := btree.NewTreeIterator(transaction.TxnNoop(), tree)
	for i := 0; i < n; i++ {
		_, val := it.Next()
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
	}
	_, val := it.Next()
	assert.Nil(t, val)
}
