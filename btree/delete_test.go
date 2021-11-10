package btree

import (
	"fmt"
	"testing"

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

func TestDelete_Internals(t *testing.T) {
	tree := NewBtreeWithPager(4, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, KeySize: 8})
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
	tree := NewBtreeWithPager(4, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, KeySize: 8})
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
