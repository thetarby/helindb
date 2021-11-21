package db_types

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

type IntegerType struct {
}

func (i *IntegerType) Less(this *Value, than *Value) bool {
	return this.GetAsInterface().(int32) < than.GetAsInterface().(int32)
}

func (i *IntegerType) Add(right *Value, left *Value) *Value {
	res := right.GetAsInterface().(int32) + left.GetAsInterface().(int32)
	return NewValue(res)
}

func (i *IntegerType) Serialize(dest []byte, src *Value) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, src.GetAsInterface().(int32))
	common.PanicIfErr(err)
	copy(dest, buf.Bytes())
}

func (i *IntegerType) Deserialize(src []byte) *Value {
	reader := bytes.NewReader(src)
	var res int32
	binary.Read(reader, binary.BigEndian, &res)
	return NewValue(res)
}

func (i *IntegerType) Length() int {
	return binary.Size(int32(1))
}

func (i *IntegerType) TypeId() uint8 {
	return 1
}
