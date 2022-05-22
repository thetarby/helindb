package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)


func TestBlock(t *testing.T) {
	block := block([]byte("\x00\x05\x00apple\x02\x05\x00ricot\x00\x06\x05bananaselam\x00\x00\x00\x00\x01\x00\x00\x00"))
	assert.Equal(t, 1, block.NumRestarts())
	
	bIt, err := block.seek(DefaultComparer, []byte("apple"))
	assert.NoError(t, err)
	
	assert.Equal(t, "apple", string(bIt.Key()))
}

func TestBlock2(t *testing.T) {
	block := block([]byte("\x00\x05\x00apple\x02\x05\x00ricot\x00\x06\x05bananaselam\x00\x00\x00\x00\x01\x00\x00\x00"))
	assert.Equal(t, 1, block.NumRestarts())
	
	bIt, err := block.seek(DefaultComparer, []byte("applee"))
	assert.NoError(t, err)
	
	assert.Equal(t, "apricot", string(bIt.Key()))
}

func TestBlock3(t *testing.T) {
	block := block([]byte("\x00\x05\x00apple\x02\x05\x00ricot\x00\x06\x05bananaselam\x00\x00\x00\x00\x01\x00\x00\x00"))
	assert.Equal(t, 1, block.NumRestarts())
	
	bIt, err := block.seek(DefaultComparer, []byte("banaa"))
	assert.NoError(t, err)
	
	assert.Equal(t, "banana", string(bIt.Key()))
	assert.Equal(t, "selam", string(bIt.Value()))
}
