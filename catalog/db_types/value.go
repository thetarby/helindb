package db_types

import "helin/btree"

type Value struct {
	typeID uint8
	value  interface{}
}

func (v *Value) Less(than btree.Key) bool {
	return v.LessThanValue(than.(*Value))
}

func (v *Value) LessThanValue(than *Value) bool {
	return GetInstance(v.GetTypeId()).Less(v, than)
}

func (v *Value) GetTypeId() uint8 {
	return v.typeID
}

func (v *Value) Serialize(dest []byte) {
	GetInstance(v.GetTypeId()).Serialize(dest, v)
}

func (v *Value) Size() int {
	return GetInstance(v.GetTypeId()).Length()
}

func Deserialize(typeID uint8, src []byte) *Value {
	return GetInstance(typeID).Deserialize(src)
}

func (v *Value) GetAsInterface() interface{} {
	return v.value
}

func NewValue(src interface{}) *Value {
	var typeID uint8
	switch src.(type) {
	case int32:
		typeID = 1
	case string:
		typeID = 2
	}

	return &Value{
		typeID: typeID,
		value:  src,
	}
}
