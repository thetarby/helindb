package db_types

import (
	"helin/common"
)

type Value struct {
	typeID TypeID
	value  interface{}
}

func (v *Value) Less(than common.Key) bool {
	return v.LessThanValue(than.(*Value))
}

func (v *Value) LessThanValue(than *Value) bool {
	return GetInstance(v.GetTypeId()).Less(v, than)
}

func (v *Value) GetTypeId() TypeID {
	return v.typeID
}

func (v *Value) Serialize(dest []byte) {
	GetInstance(v.GetTypeId()).Serialize(dest, v)
}

func (v *Value) Size() int {
	return GetInstance(v.GetTypeId()).Length()
}

func Deserialize(typeID TypeID, src []byte) *Value {
	return GetInstance(typeID).Deserialize(src)
}

func (v *Value) GetAsInterface() interface{} {
	return v.value
}

func NewValue(src interface{}) *Value {
	var typeID TypeID
	switch src.(type) {
	case int32:
		typeID = TypeID{
			KindID: 1,
			Size:   0,
		}
	case string:
		typeID = TypeID{
			KindID: 2,
			Size:   0,
		}
	case []byte:
		typeID = TypeID{
			KindID: 3,
			Size:   uint32(len(src.([]byte))),
		}
	case bool:
		typeID = TypeID{
			KindID: 4,
			Size:   1,
		}
	default:
		panic("not supported type")
	}

	return &Value{
		typeID: typeID,
		value:  src,
	}
}
