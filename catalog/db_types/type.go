package db_types

type TypeID struct {
	KindID uint8
	Size   uint32 // for fixed len array like types such as char[20], int[10]
}

// DbType is the interface that should be implemented to make a struct supported by the db.
type DbType interface {
	Less(this *Value, Than *Value) bool
	Add(right *Value, left *Value) *Value
	Serialize(dest []byte, src *Value)
	Deserialize(src []byte) *Value

	// Length should return the size of the bytes when value is serialized
	Length(val *Value) int

	TypeId() TypeID
}

func GetType(typeID TypeID) DbType {
	switch typeID.KindID {
	case 1:
		return &IntegerType{}
	case 2:
		return &CharType{}
	case 3:
		return &FixedLenCharType{
			Size: typeID.Size,
		}
	case 4:
		return &Float64Type{}
	default:
		return nil
	}
}
