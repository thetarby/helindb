package freelistv1

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFreeList(t *testing.T) {
	fl := NewFreeList(newMemPager(), true)

	fl.GetHeaderPageLsn()

	assert.NoError(t, fl.Add(nil, 10))
	assert.NoError(t, fl.Add(nil, 20))
	assert.NoError(t, fl.Add(nil, 30))

	popped, err := fl.Pop(nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 10, popped)

	popped, err = fl.Pop(nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 20, popped)

	popped, err = fl.Pop(nil)
	assert.NoError(t, err)
	assert.EqualValues(t, 30, popped)
}
