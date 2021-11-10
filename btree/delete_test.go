package btree

import (
	"fmt"
	"helin/buffer"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty(t *testing.T) {
	tree := NewBtree(4)
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(MyInt(val), "selam")
	}
	var stack []NodeIndexPair
	res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(MyInt(1), stack)
	assert.Len(t, stack, 3)
	assert.Equal(t, "selam", res.(string))

	tree.Delete(MyInt(1))
	stack = []NodeIndexPair{}
	_, stack = tree.pager.GetNode(tree.Root).findAndGetStack(MyInt(1), stack)

	assert.Len(t, stack, 2)
}

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty_3(t *testing.T) {
	tree := NewBtreeWithPager(4, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}})
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(PersistentKey(val), SlotPointer{
			PageId:  10,
			SlotIdx: 10,
		})
	}
	var stack []NodeIndexPair
	res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(1), stack)
	assert.Len(t, stack, 3)
	assert.Equal(t, SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}, res.(SlotPointer))

	tree.Delete(PersistentKey(1))
	stack = []NodeIndexPair{}
	_, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(1), stack)

	assert.Len(t, stack, 2)
}

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty_2(t *testing.T) {
	tree := NewBtree(3)

	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(MyInt(val), "selam")
	}

	var stack []NodeIndexPair
	res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(MyInt(1), stack)
	assert.Len(t, stack, 4)
	assert.Equal(t, "selam", res.(string))

	tree.Print()
	for _, i := range []int{1, 2, 3, 4, 5} {
		var stack []NodeIndexPair
		tree.Delete(MyInt(i))
		tree.Print()
		res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(MyInt(10), stack)
		assert.Len(t, stack, 3)
		assert.Equal(t, "selam", res.(string))
	}

	tree.Delete(MyInt(6))
	stack = []NodeIndexPair{}
	_, stack = tree.pager.GetNode(tree.Root).findAndGetStack(MyInt(10), stack)
	assert.Len(t, stack, 2)
}

func TestPersistentDeleted_Items_Should_Not_Be_Found(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	dbFile := uuid.New().String() + ".helin"
	pool := buffer.NewBufferPool(dbFile)
	defer os.Remove(dbFile)
	tree := NewBtreeWithPager(100, NewBufferPoolPager(pool, &PersistentKeySerializer{}))

	n := 100000
	for num, i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
		println("inserted %v", num, i, tree.Height(), tree.pager.(*BufferPoolPager).pool.EmptyFrameSize())
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
		println("deleted %v", i)

		val = tree.Find(PersistentKey(i))
		assert.Nil(t, val)
	}
}

func TestDelete_Internals(t *testing.T) {
	tree := NewBtreeWithPager(4, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}})
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	tree.Insert(PersistentKey(1), p)
	tree.Insert(PersistentKey(5), p)
	tree.Insert(PersistentKey(3), p)
	tree.Insert(PersistentKey(2), p)
	tree.Print()
	for _, val := range []int{81, 87, 47, 59, 82, 88, 89} {
		tree.Insert(PersistentKey(val), p)
		fmt.Println("new tree: !!!")
		tree.Print()
	}

	tree.Print()

	tree.Delete(PersistentKey(3))
	fmt.Println("After Delete 3 !!!!!!!!!!!!!!!!!!!!!!!!!!!")
	tree.Print()
	tree.Delete(PersistentKey(5))
	fmt.Println("After Delete 5 !!!!!!!!!!!!!!!!!!!!!!!!!!!")
	tree.Print()
	tree.Delete(PersistentKey(1))
	fmt.Println("After Delete 1 !!!!!!!!!!!!!!!!!!!!!!!!!!!")
	tree.Print()
}

func TestDelete_Internals2(t *testing.T) {
	tree := NewBtreeWithPager(4, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}})
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(PersistentKey(val), p)
		fmt.Println("new tree: !!!")
		tree.Print()
	}

	tree.Delete(PersistentKey(9))
	fmt.Println("After Delete 9 !!!!!!!!!!!!!!!!!!!!!!!!!!!")
	tree.Print()
}
