package db_types

import (
	"fmt"
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
	return GetType(v.GetTypeId()).Less(v, than)
}

func (v *Value) GetTypeId() TypeID {
	return v.typeID
}

func (v *Value) Serialize(dest []byte) {
	GetType(v.GetTypeId()).Serialize(dest, v)
}

// Size returns the size of the value when it is serialized.
func (v *Value) Size() int {
	return GetType(v.GetTypeId()).Length(v)
}

func (v *Value) String() string {
	return fmt.Sprintf("%v", v.value)
}

func Deserialize(typeID TypeID, src []byte) *Value {
	return GetType(typeID).Deserialize(src)
}

func (v *Value) GetAsInterface() interface{} {
	return v.value
}

func NewValue(src interface{}) *Value {
	var typeID TypeID
	switch src.(type) {
	case int32:
		typeID = IntegerTypeID
	case string:
		typeID = CharTypeID
	case float64:
		typeID = Float64TypeID
	case bool:
		typeID = BoolTypeID
	default:
		panic("not supported type")
	}

	return &Value{
		typeID: typeID,
		value:  src,
	}
}

func V(src interface{}) *Value {
	return NewValue(src)
}
