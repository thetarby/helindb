package catalog

import (
	"helin/btree"
	"helin/catalog/db_types"
	"helin/disk/structures"
)

type CharTypeKeySerializer struct{
	KeySize int
}

func (p *CharTypeKeySerializer) Serialize(key btree.Key) ([]byte, error) {
	val := key.(*db_types.Value)
	res := make([]byte, val.Size())
	val.Serialize(res)

	return res, nil
}

func (p *CharTypeKeySerializer) Deserialize(data []byte) (btree.Key, error) {
	var val = db_types.Deserialize(db_types.TypeID{
		KindID: 2,
	}, data)
	return val, nil
}

func (p *CharTypeKeySerializer) Size() int {
	return p.KeySize
}

// TupleKeySerializer serializes a TupleKey with a schema. Since each db value can be 
// serialized to binary all TupleKeySerializer has to do is to get each column from tuple
// and serialize in order 
type TupleKeySerializer struct{
	schema Schema
	keySize int
}

func (p *TupleKeySerializer) Serialize(key btree.Key) ([]byte, error) {
	tupleKey := key.(*TupleKey)

	dest := make([]byte, 0)
	for i, _ := range tupleKey.Schema.GetColumns() {
		val := tupleKey.GetValue(tupleKey.Schema, i)
		size := val.Size()
		temp := make([]byte, size)
		val.Serialize(temp)
		dest = append(dest, temp...)
	}

	return dest, nil
}

func (p *TupleKeySerializer) Deserialize(data []byte) (btree.Key, error) {
	row := structures.Row{
		Data: data[:p.keySize],
		Rid:  structures.Rid{},
	}

	tuple := CastRowAsTuple(&row)

	return &TupleKey{
		Schema: p.schema,
		Tuple:  *tuple,
	}, nil
}

func (p *TupleKeySerializer) Size() int {
	return p.keySize
}