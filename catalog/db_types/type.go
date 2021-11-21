package db_types

// DbType is the interface that should be implemented to make a struct supported by the db.
type DbType interface {
	Less(this *Value, Than *Value) bool
	Add(right *Value, left *Value) *Value
	Serialize(dest []byte, src *Value)
	Deserialize(src []byte) *Value
	Length() int

	TypeId() uint8
}

func GetInstance(typeID uint8) DbType {
	switch typeID {
	case 1:
		return &IntegerType{}
	case 2:
		return &CharType{}
	default:
		return nil
	}
}
