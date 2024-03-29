package btree

import (
	"helin/buffer"
	"helin/common"
	"helin/transaction"
	io2 "io"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty_3(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 4, NewMemPager(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(transaction.TxnNoop(), PersistentKey(val), SlotPointer{
			PageId:  10,
			SlotIdx: 10,
		})
	}

	res, stack := tree.FindAndGetStack(PersistentKey(1), Debug)
	release(stack)
	assert.Len(t, stack, 3)
	assert.Equal(t, SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}, res.(SlotPointer))

	tree.Delete(transaction.TxnNoop(), PersistentKey(1))
	_, stack = tree.FindAndGetStack(PersistentKey(1), Debug)
	release(stack)
	assert.Len(t, stack, 2)
}

func TestPersistentDeleted_Items_Should_Not_Be_Found(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	dbFile := uuid.New().String() + ".helin"
	pool := buffer.NewBufferPool(dbFile, 64)
	defer common.Remove(dbFile)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 100, NewDefaultBPP(pool, &PersistentKeySerializer{}, io2.Discard))

	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
		// println("inserted %v", num, i, tree.Height(), tree.pager.(*BufferPoolPager).pool.EmptyFrameSize())
	}

	for i := 0; i < n; i++ {
		val := tree.Find(PersistentKey(i))
		if val == nil {
			tree.Find(PersistentKey(i))
			tree.Print()
		}
		assert.Equal(t, SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		}, val.(SlotPointer))
		tree.Delete(transaction.TxnNoop(), PersistentKey(i))
		// println("deleted %v", i)

		val = tree.Find(PersistentKey(i))
		assert.Nil(t, val)
	}
}

func TestPersistentPin_Count_Should_Be_Zero_After_Deletes_Succeeds(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	dbFile := uuid.New().String() + ".helin"
	pool := buffer.NewBufferPool(dbFile, 16)
	defer common.Remove(dbFile)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewDefaultBPP(pool, &PersistentKeySerializer{}, io2.Discard))

	n := 1000
	rand.Seed(42)
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
		if pool.Replacer.NumPinnedPages() > 0 {
			t.Error("# of pinned pages is not 0")
		}
	}

	assert.Equal(t, 0, pool.Replacer.NumPinnedPages())

	for i := 0; i < n; i++ {
		tree.Delete(transaction.TxnNoop(), PersistentKey(i))
		if pool.Replacer.NumPinnedPages() > 0 {
			t.Errorf("# of pinned pages is not 0, it is :%v", pool.Replacer.NumPinnedPages())
		}
	}

	assert.Equal(t, 0, pool.Replacer.NumPinnedPages())
}
