package btree

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHeight_Should_Return_Correct_Height(t *testing.T) {
	tests := []struct {
		tree     *BTree
		toInsert []PersistentKey
		expected int
	}{
		{
			tree:     NewBtreeWithPager(3, NewNoopPagerWithValueSize(&PersistentKeySerializer{}, &StringValueSerializer{Len: 5})),
			toInsert: []PersistentKey{PersistentKey(1), PersistentKey(2), PersistentKey(3), PersistentKey(4), PersistentKey(5), PersistentKey(6), PersistentKey(7), PersistentKey(8), PersistentKey(9)},
			expected: 4,
		},
		{
			tree:     NewBtreeWithPager(4, NewNoopPagerWithValueSize(&PersistentKeySerializer{}, &StringValueSerializer{Len: 5})),
			toInsert: []PersistentKey{PersistentKey(1), PersistentKey(2), PersistentKey(3), PersistentKey(4)},
			expected: 2,
		},
		{
			tree:     NewBtreeWithPager(5, NewNoopPagerWithValueSize(&PersistentKeySerializer{}, &StringValueSerializer{Len: 5})),
			toInsert: []PersistentKey{PersistentKey(1), PersistentKey(2), PersistentKey(3), PersistentKey(4), PersistentKey(5)},
			expected: 2,
		},
	}
	for _, test := range tests {
		for _, m := range test.toInsert {
			test.tree.Insert(m, "value")
		}
		h := test.tree.Height()
		assert.Equal(t, test.expected, h)

	}
}
