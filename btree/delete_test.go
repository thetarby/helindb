package btree

import (
	"helin/transaction"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 4, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(transaction.TxnNoop(), PersistentKey(val), "selam")
	}

	res, stack := tree.FindAndGetStack(PersistentKey(1), Read)
	tree.runlatch(stack)
	assert.Len(t, stack, 3)
	assert.Equal(t, "selam", res.(string))

	tree.Delete(transaction.TxnNoop(), PersistentKey(1))
	_, stack = tree.FindAndGetStack(PersistentKey(1), Read)

	assert.Len(t, stack, 2)
}

func TestDelete_Should_Decrease_Height_Size_When_Root_Is_Empty_2(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(transaction.TxnNoop(), PersistentKey(val), "selam")
	}

	res, stack := tree.FindAndGetStack(PersistentKey(1), Read)
	tree.runlatch(stack)
	assert.Len(t, stack, 4)
	assert.Equal(t, "selam", res.(string))

	for _, i := range []int{1, 2, 3, 4, 5} {
		tree.Delete(transaction.TxnNoop(), PersistentKey(i))
		res, stack := tree.FindAndGetStack(PersistentKey(10), Read)
		tree.runlatch(stack)
		assert.Len(t, stack, 3)
		assert.Equal(t, "selam", res.(string))
	}

	tree.Delete(transaction.TxnNoop(), PersistentKey(6))
	_, stack = tree.FindAndGetStack(PersistentKey(10), Read)
	tree.runlatch(stack)
	assert.Len(t, stack, 2)
}

func TestDelete_Internals(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 4, NewMemPager(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	tree.Insert(transaction.TxnNoop(), PersistentKey(1), p)
	tree.Insert(transaction.TxnNoop(), PersistentKey(5), p)
	tree.Insert(transaction.TxnNoop(), PersistentKey(3), p)
	tree.Insert(transaction.TxnNoop(), PersistentKey(2), p)
	//tree.Print()
	for _, val := range []int{81, 87, 47, 59, 82, 88, 89} {
		tree.Insert(transaction.TxnNoop(), PersistentKey(val), p)
	}

	tree.Delete(transaction.TxnNoop(), PersistentKey(3))
	//tree.Print()
	tree.Delete(transaction.TxnNoop(), PersistentKey(5))
	//tree.Print()
	tree.Delete(transaction.TxnNoop(), PersistentKey(1))
	//tree.Print()
}

func TestDelete_Internals2(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 4, NewMemPager(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))

	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	for _, val := range []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10} {
		tree.Insert(transaction.TxnNoop(), PersistentKey(val), p)
		//tree.Print()
	}

	tree.Delete(transaction.TxnNoop(), PersistentKey(9))
	//tree.Print()
}
