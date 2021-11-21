package structures

type ITuple interface {
	Serialize(dest []byte)
	Deserialize(from []byte)
	GetRid() Rid
	GetData() []byte
	Length() int
}

type Tuple struct {
	data []byte
	rid  Rid
}

func (t *Tuple) Serialize(dest []byte) {
	copy(dest, t.data)
}

func (t *Tuple) Deserialize(from []byte) {
	copy(t.data, from)
}

func (t *Tuple) GetRid() Rid {
	return t.rid
}

func (t *Tuple) GetData() []byte {
	return t.data
}

func (t *Tuple) Length() int {
	return len(t.data)
}
