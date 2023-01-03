package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty(t *testing.T) {
	tree := NewBtreeWithPager(4, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(PersistentKey(val), "selam")
	}

	res, stack := tree.FindAndGetStack(PersistentKey(1), Read)
	tree.runlatch(stack)
	assert.Len(t, stack, 3)
	assert.Equal(t, "selam", res.(string))

	tree.Delete(PersistentKey(1))
	_, stack = tree.FindAndGetStack(PersistentKey(1), Read)

	assert.Len(t, stack, 2)
}

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty_2(t *testing.T) {
	tree := NewBtreeWithPager(3, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(PersistentKey(val), "selam")
	}

	res, stack := tree.FindAndGetStack(PersistentKey(1), Read)
	tree.runlatch(stack)
	assert.Len(t, stack, 4)
	assert.Equal(t, "selam", res.(string))

	for _, i := range []int{1, 2, 3, 4, 5} {
		tree.Delete(PersistentKey(i))
		res, stack := tree.FindAndGetStack(PersistentKey(10), Read)
		tree.runlatch(stack)
		assert.Len(t, stack, 3)
		assert.Equal(t, "selam", res.(string))
	}

	tree.Delete(PersistentKey(6))
	_, stack = tree.FindAndGetStack(PersistentKey(10), Read)
	tree.runlatch(stack)
	assert.Len(t, stack, 2)
}

func TestDelete_Internals(t *testing.T) {
	tree := NewBtreeWithPager(4, NewMemPager(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	tree.Insert(PersistentKey(1), p)
	tree.Insert(PersistentKey(5), p)
	tree.Insert(PersistentKey(3), p)
	tree.Insert(PersistentKey(2), p)
	//tree.Print()
	for _, val := range []int{81, 87, 47, 59, 82, 88, 89} {
		tree.Insert(PersistentKey(val), p)
	}

	tree.Delete(PersistentKey(3))
	//tree.Print()
	tree.Delete(PersistentKey(5))
	//tree.Print()
	tree.Delete(PersistentKey(1))
	//tree.Print()
}

func TestDelete_Internals2(t *testing.T) {
	tree := NewBtreeWithPager(4, NewMemPager(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))

	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(PersistentKey(val), p)
		//tree.Print()
	}

	tree.Delete(PersistentKey(9))
	//tree.Print()
}
