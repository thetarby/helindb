package structures

type ITuple interface {
	Serialize(dest []byte)
	Deserialize(from []byte)
	GetRid() Rid
	GetData() []byte
	Length() int
}

type Tuple struct {
	Data []byte
	Rid  Rid
}

func (t *Tuple) Serialize(dest []byte) {
	copy(dest, t.Data)
}

func (t *Tuple) Deserialize(from []byte) {
	copy(t.Data, from)
}

func (t *Tuple) GetRid() Rid {
	return t.Rid
}

func (t *Tuple) GetData() []byte {
	return t.Data
}

func (t *Tuple) Length() int {
	return len(t.Data)
}
