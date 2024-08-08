package btree

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"helin/transaction"
	"math/rand"
	"testing"
)

func TestTreeIterator_Should_Return_Every_Value_Bigger_Than_Or_Equal_To_Key_When_Initialized_With_A_Key(t *testing.T) {
	pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
	}

	it := NewTreeIteratorWithKey(transaction.TxnNoop(), StringKey("selam_099"), tree)
	i := 9900
	for _, val := it.Next(); val != nil; _, val = it.Next() {
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
		i++
	}
}

func TestTreeIterator_Should_Return_All_Values_When_Initialized_Without_A_Key(t *testing.T) {
	pager2 := NewPager2(NewMemBPager(4096*2), &StringKeySerializer{}, &StringValueSerializer{})
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, pager2)

	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
	}

	it := NewTreeIterator(transaction.TxnNoop(), tree)
	for i := 0; i < n; i++ {
		_, val := it.Next()
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
	}
	_, val := it.Next()
	assert.Nil(t, val)
}
