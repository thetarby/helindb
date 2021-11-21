package db_types

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCharType_Serialize(t *testing.T) {
	val := NewValue("this is a char type")

	dest := make([]byte, 100)
	val.Serialize(dest)

	readVal := Deserialize(val.GetTypeId(), dest)

	assert.Equal(t, val.GetAsInterface().(string), readVal.GetAsInterface().(string))
	assert.Equal(t, "this is a char type", readVal.GetAsInterface().(string))
}
