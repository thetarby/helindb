package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func b(s string) []byte{
	return []byte(s)
}

func TestSharedPrefixLen(t *testing.T){
	assert.Equal(t, 5, SharedPrefixLen(b("selam"), b("selami")))
	assert.Equal(t, 5, SharedPrefixLen(b("selam"), b("selam")))
	assert.Equal(t, 4, SharedPrefixLen(b("selam"), b("selan")))
	assert.Equal(t, 3, SharedPrefixLen(b("selam"), b("selüm")))
	assert.Equal(t, 0, SharedPrefixLen(b(""), b("selüm")))
	assert.Equal(t, 0, SharedPrefixLen(b("asd"), b("")))
}