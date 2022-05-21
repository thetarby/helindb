package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBlockIter(t *testing.T) {
	block := []byte("\x00\x05\x00apple\x02\x05\x00ricot\x00\x06\x05bananaselam")
	bit := NewBlockIter(block)

	// Key and Value methods should return nil when Next is not called yet
	assert.Nil(t, bit.Key())
	assert.Nil(t, bit.Value())

	assert.True(t, bit.Next())
	assert.Equal(t, "apple", string(bit.Key()))
	
	assert.True(t, bit.Next())
	assert.Equal(t, "apricot", string(bit.Key()))
	
	assert.True(t, bit.Next())
	assert.Equal(t, "banana", string(bit.Key()))
	assert.Equal(t, "selam", string(bit.Value()))

	// should return false after block is finished
	assert.False(t, bit.Next())
}

func TestNewBlockIterFrom_Should_Return_Done_When_All_Keys_In_Block_Are_Smaller_Than_Sought_Key(t *testing.T) {
	block := []byte("\x00\x05\x00apple\x02\x05\x00ricot\x00\x06\x05bananaselam")
	bit, _ := NewBlockIterFrom(block, b("z"), DefaultComparer)
	assert.True(t, bit.done)
}