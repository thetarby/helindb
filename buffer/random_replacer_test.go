package buffer

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestRandomReplacerShouldWork(t *testing.T) {
	r := NewRandomReplacer()
	r.Pin(1)
	r.Pin(2)
	v, _ := r.ChooseVictim()
	assert.Less(t, v, PoolSize)
}

func TestRandomReplacerShouldReturnError_When_No_Possible_Victim_Is_Found(t *testing.T) {
	r := NewRandomReplacer()
	for i := 0; i < PoolSize; i++ {
		r.Pin(i)
	}
	v, err := r.ChooseVictim()
	assert.Zero(t, v)
	assert.Error(t, err)
}

func TestRandomReplacer_Should_Not_Choose_Pinned(t *testing.T) {
	r := NewRandomReplacer()
	for i := 0; i < PoolSize-1; i++ {
		r.Pin(i)
	}
	v, err := r.ChooseVictim()
	assert.NoError(t, err)
	assert.Equal(t, PoolSize-1, v)
}

func TestRandomReplacer_Unpin_Should_Work(t *testing.T) {
	r := NewRandomReplacer()
	for i := 0; i < PoolSize; i++ {
		r.Pin(i)
	}
	r.Unpin(PoolSize - 1)
	v, err := r.ChooseVictim()
	assert.NoError(t, err)
	assert.Equal(t, PoolSize-1, v)
}
