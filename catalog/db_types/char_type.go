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
	l := uint32(len(str))

	// first write size
	err := binary.Write(&buf, binary.BigEndian, l)
	common.PanicIfErr(err)

	// then write str itself
	err = binary.Write(&buf, binary.BigEndian, []byte(str))
	common.PanicIfErr(err)
	copy(dest, buf.Bytes())
}

func (c *CharType) Deserialize(src []byte) *Value {
	reader := bytes.NewReader(src)
	var l uint32
	err := binary.Read(reader, binary.BigEndian, &l)
	common.PanicIfErr(err)
	uint32Size := binary.Size(l)
	str := string(src[uint32Size : uint32Size+int(l)])
	return NewValue(str)
}

func (c *CharType) Length() int {
	//return c.Size
	// TODO: char'ı daha güzel hallet
	return 20
}

func (c *CharType) TypeId() TypeID {
	return CharTypeID
}
