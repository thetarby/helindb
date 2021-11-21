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
	Length() int

	TypeId() TypeID
}

func GetInstance(typeID TypeID) DbType {
	switch typeID.KindID {
	case 1:
		return &IntegerType{}
	case 2:
		return &CharType{}
	case 3:
		return &FixedLenCharType{
			Size: typeID.Size,
		}
	default:
		return nil
	}
}
