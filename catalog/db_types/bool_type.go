package db_types

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

var BoolTypeID = TypeID{
	KindID: 5,
	Size:   1,
}

type BoolType struct {
}

func (i *BoolType) Less(this *Value, than *Value) bool {
	return this.GetAsInterface().(uint8) < than.GetAsInterface().(uint8)
}

func (i *BoolType) Add(right *Value, left *Value) *Value {
	res := right.GetAsInterface().(uint8) + left.GetAsInterface().(uint8)
	return NewValue(res)
}

func (i *BoolType) Serialize(dest []byte, src *Value) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, src.GetAsInterface().(uint8))
	common.PanicIfErr(err)
	copy(dest, buf.Bytes())
}

func (i *BoolType) Deserialize(src []byte) *Value {
	reader := bytes.NewReader(src)
	var res uint8
	common.PanicIfErr(binary.Read(reader, binary.BigEndian, &res))
	return NewValue(res)
}

func (i *BoolType) Length(*Value) int {
	return 1
}

func (i *BoolType) TypeId() TypeID {
	return BoolTypeID
}
