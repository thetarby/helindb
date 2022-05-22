package sstable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func b(s string) []byte{return []byte(s)}

func TestDefaultComparer(t *testing.T){
	cmp := DefaultComparer
	assert.Equal(t, b("selb"), cmp.Separator(b("selam"), b("selim")))
	assert.Equal(t, b(""), cmp.Separator(b(""), b("selam")))
	assert.Equal(t, b("abc"), cmp.Separator(b("abc"), b("abcd")))
	assert.Equal(t, b("00123"), cmp.Separator(b("00123"), b("00124")))
	assert.Equal(t, b("b"), cmp.Separator(b("abc"), b("def")))
	assert.Equal(t, b("1345"), cmp.Separator(b("1345"), b("2")))
}