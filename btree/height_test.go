package btree

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHeight_Should_Return_Correct_Height(t *testing.T) {
	tests := []struct {
		tree     *BTree
		toInsert []MyInt
		expected int
	}{
		{
			tree:     NewBtree(3),
			toInsert: []MyInt{MyInt(1), MyInt(2), MyInt(3), MyInt(4), MyInt(5), MyInt(6), MyInt(7), MyInt(8), MyInt(9)},
			expected: 4,
		},
		{
			tree:     NewBtree(4),
			toInsert: []MyInt{MyInt(1), MyInt(2), MyInt(3), MyInt(4)},
			expected: 2,
		},
		{
			tree:     NewBtree(5),
			toInsert: []MyInt{MyInt(1), MyInt(2), MyInt(3), MyInt(4), MyInt(5)},
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
