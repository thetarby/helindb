package structures

/*
	Row type corresponds to each row in a table heap. Row type does not care about the content of the information it is
	keeping. It just sees its content as a binary byte array. No information about schema or metadata is kept in row object.

	Schema related info is kept in catalog.Tuple type which is derived from Row type
*/

type IRow interface {
	Serialize(dest []byte)
	Deserialize(from []byte)
	GetRid() Rid
	GetData() []byte
	Length() int
}

// Row corresponds to each record in a table at the lowest level. It does not care about
// its content and sees it as byte array only. It has Rid which should be unique for every
// row and acts as an address for the Row.
type Row struct {
	Data []byte
	Rid  Rid
}

func (t *Row) Serialize(dest []byte) {
	copy(dest, t.Data)
}

func (t *Row) Deserialize(from []byte) {
	copy(t.Data, from)
}

func (t *Row) GetRid() Rid {
	return t.Rid
}

func (t *Row) GetData() []byte {
	return t.Data
}

func (t *Row) Length() int {
	return len(t.Data)
}
