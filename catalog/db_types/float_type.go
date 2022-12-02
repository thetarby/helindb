package db_types

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

var Float64TypeID = TypeID{
	KindID: 4,
	Size:   8,
}

type Float64Type struct {
}

func (i *Float64Type) Less(this *Value, than *Value) bool {
	return this.GetAsInterface().(float64) < than.GetAsInterface().(float64)
}

func (i *Float64Type) Add(right *Value, left *Value) *Value {
	res := right.GetAsInterface().(float64) + left.GetAsInterface().(float64)
	return NewValue(res)
}

func (i *Float64Type) Serialize(dest []byte, src *Value) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, src.GetAsInterface().(float64))
	common.PanicIfErr(err)
	copy(dest, buf.Bytes())
}

func (i *Float64Type) Deserialize(src []byte) *Value {
	reader := bytes.NewReader(src)
	var res float64
	common.PanicIfErr(binary.Read(reader, binary.BigEndian, &res))
	return NewValue(res)
}

func (i *Float64Type) Length(*Value) int {
	return 8
}

func (i *Float64Type) TypeId() TypeID {
	return Float64TypeID
}
