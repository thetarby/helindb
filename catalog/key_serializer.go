package catalog

import (
	"helin/catalog/db_types"
	"helin/common"
	"helin/disk/structures"
)

type CharTypeKeySerializer struct {
	KeySize int
}

func (p *CharTypeKeySerializer) Serialize(key common.Key) ([]byte, error) {
	val := key.(*db_types.Value)
	res := make([]byte, val.Size())
	val.Serialize(res)

	return res, nil
}

func (p *CharTypeKeySerializer) Deserialize(data []byte) (common.Key, error) {
	var val = db_types.Deserialize(db_types.CharTypeID, data)
	return val, nil
}

func (p *CharTypeKeySerializer) Size() int {
	return p.KeySize
}

// TupleKeySerializer serializes a TupleKey with a schema. Since each db value can be
// serialized to binary all TupleKeySerializer has to do is to get each column from tuple
// and serialize in order
type TupleKeySerializer struct {
	schema Schema
}

func (p *TupleKeySerializer) Serialize(key common.Key) ([]byte, error) {
	tupleKey := key.(*TupleKey)

	return tupleKey.GetData(), nil
}

func (p *TupleKeySerializer) Deserialize(data []byte) (common.Key, error) {
	copied := make([]byte, len(data))
	copy(copied, data)
	row := structures.Row{
		Data: copied,
	}

	tuple := CastRowAsTuple(&row)

	return &TupleKey{
		Schema: p.schema,
		Tuple:  *tuple,
	}, nil
}

func (p *TupleKeySerializer) Size() int {
	return -1
}
