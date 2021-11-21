package db_types

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

type FixedLenCharType struct {
	Size uint32
}

func (c *FixedLenCharType) Less(this *Value, than *Value) bool {
	return this.GetAsInterface().(string) < than.GetAsInterface().(string)
}

func (c *FixedLenCharType) Add(right *Value, left *Value) *Value {
	panic("implement me")
}

func (c *FixedLenCharType) Serialize(dest []byte, src *Value) {
	buf := bytes.Buffer{}
	str := src.GetAsInterface().(string)
	err := binary.Write(&buf, binary.BigEndian, []byte(str))
	common.PanicIfErr(err)
	copy(dest, buf.Bytes())
}

func (c *FixedLenCharType) Deserialize(src []byte) *Value {
	str := string(src[:c.Size])
	return NewValue(str)
}

func (c *FixedLenCharType) Length() int {
	return int(c.Size)
}

func (c *FixedLenCharType) TypeId() TypeID {
	return TypeID{
		KindID: 3,
		Size:   c.Size,
	}
}
