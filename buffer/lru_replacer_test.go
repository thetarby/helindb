package buffer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLruReplacerShouldReturnError_When_No_Possible_Victim_Is_Found(t *testing.T) {
	PoolSize := 32
	r := NewLruReplacer(PoolSize)
	for i := 0; i < PoolSize; i++ {
		r.Pin(i)
	}
	v, err := r.ChooseVictim()
	assert.Zero(t, v)
	assert.Error(t, err)
}

func TestLruReplacer_Should_Not_Choose_Pinned(t *testing.T) {
	PoolSize := 32
	r := NewLruReplacer(PoolSize)
	for i := 0; i < PoolSize; i++ {
		r.Pin(i)
	}
	r.Unpin(PoolSize - 1)
	v, err := r.ChooseVictim()
	assert.NoError(t, err)
	assert.Equal(t, PoolSize-1, v)
}
