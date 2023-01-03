package db_types

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

var CharTypeID = TypeID{
	KindID: 2,
	Size:   0,
}

type CharType struct {
}

func (c *CharType) Less(this *Value, than *Value) bool {
	return this.GetAsInterface().(string) < than.GetAsInterface().(string)
}

func (c *CharType) Add(right *Value, left *Value) *Value {
	panic("implement me")
}

func (c *CharType) Serialize(dest []byte, src *Value) {
	buf := bytes.Buffer{}
	str := src.GetAsInterface().(string)

	// then write str itself
	err := binary.Write(&buf, binary.BigEndian, []byte(str))
	common.PanicIfErr(err)
	copy(dest, buf.Bytes())
}

func (c *CharType) Deserialize(src []byte) *Value {
	str := string(src)
	return NewValue(str)
}

func (c *CharType) Length(v *Value) int {
	s := v.value.(string)
	return len([]byte(s))
}

func (c *CharType) TypeId() TypeID {
	return CharTypeID
}
