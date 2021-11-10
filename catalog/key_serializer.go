package catalog

import (
	"fmt"
	"helin/btree"
	"helin/catalog/db_types"
	"helin/disk/structures"
	strings2 "strings"
)

type CharTypeKeySerializer struct{}

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

type TupleKey struct {
	Schema Schema
	Tuple
}

func (t *TupleKey) String() string {
	strings := make([]string, 0)
	for idx, _ := range t.Schema.GetColumns() {
		val := t.GetValue(t.Schema, idx)
		strings = append(strings, fmt.Sprintf("%v", val.GetAsInterface()))
	}
	return strings2.Join(strings, "-")
}
func (t *TupleKey) Less(than btree.Key) bool {
	thanAsTupleKey := than.(*TupleKey)
	for idx, _ := range t.Schema.GetColumns(){
		val1 := t.GetValue(t.Schema, idx)
		if val1 == nil{
			break
		}

		val2 := thanAsTupleKey.GetValue(t.Schema, idx)
		if val2 == nil{
			break
		}

		if val1.Less(val2){
			return true
		}else if val2.Less(val1){
			return false
		}
	}

	return false
}


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