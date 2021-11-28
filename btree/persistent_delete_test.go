package btree

import (
	"helin/buffer"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty_3(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	tree := NewBtreeWithPager(4, NewNoopPagerWithValueSize(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(PersistentKey(val), SlotPointer{
			PageId:  10,
			SlotIdx: 10,
		})
	}
	var stack []NodeIndexPair
	res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(1), stack, Insert)
	assert.Len(t, stack, 3)
	assert.Equal(t, SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}, res.(SlotPointer))

	tree.Delete(PersistentKey(1))
	stack = []NodeIndexPair{}
	_, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(1), stack, Insert)

	assert.Len(t, stack, 2)
}

func TestPersistentDeleted_Items_Should_Not_Be_Found(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	dbFile := uuid.New().String() + ".helin"
	pool := buffer.NewBufferPool(dbFile, 64)
	defer os.Remove(dbFile)
	tree := NewBtreeWithPager(100, NewBufferPoolPager(pool, &PersistentKeySerializer{}))

	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
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
			PageId:  int64(i),
			SlotIdx: int16(i),
		}, val.(SlotPointer))
		tree.Delete(PersistentKey(i))
		// println("deleted %v", i)

		val = tree.Find(PersistentKey(i))
		assert.Nil(t, val)
	}
}

func TestPersistentPin_Count_Should_Be_Zero_After_Deletes_Succeeds(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	dbFile := uuid.New().String() + ".helin"
	pool := buffer.NewBufferPool(dbFile, 16)
	defer os.Remove(dbFile)
	tree := NewBtreeWithPager(10, NewBufferPoolPager(pool, &PersistentKeySerializer{}))

	n := 1000
	for _, i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}

	assert.Equal(t, 0, pool.Replacer.NumPinnedPages())

	for i := 0; i < n; i++ {
		tree.Delete(PersistentKey(i))
	}

	assert.Equal(t, 0, pool.Replacer.NumPinnedPages())
}
