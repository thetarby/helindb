package catalog

import (
	"encoding/binary"
	"errors"
	"helin/catalog/db_types"
	"helin/disk/structures"
)

type ITuple interface {
	GetValue(schema Schema, columnIdx int) *db_types.Value
}

// Tuple inherits from row. Row does not care about its content and sees it as bytes only
// however Tuple can interpret its content given a schema. A row can be converted to a
// tuple with CastRowAsTuple function.
type Tuple struct {
	structures.Row
}

func (t *Tuple) GetValue(schema Schema, columnIdx int) *db_types.Value {
	col := schema.GetColumn(columnIdx)
	data := t.GetData()
	if col.IsInlined() {
		if int(col.Offset) >= len(data) {
			return nil
		}

		return db_types.Deserialize(col.TypeId, data[col.Offset:])
	}

	l := binary.BigEndian.Uint16(data[col.Offset:])
	s := binary.BigEndian.Uint16(data[col.Offset+2:])
	if len(data) == int(s+l) {
		return db_types.Deserialize(col.TypeId, data[s:])
	}
	return db_types.Deserialize(col.TypeId, data[s:s+l])
}

func (t *Tuple) GetRow() *structures.Row {
	return &t.Row
}

func (t *Tuple) getColData(schema Schema, columnIdx int) []byte {
	col := schema.GetColumn(columnIdx)
	d := t.GetData()
	if col.IsInlined() {
		return d[col.Offset:]
	}

	offset := binary.BigEndian.Uint16(d[col.Offset:])
	return d[offset:]
}

func CastRowAsTuple(row *structures.Row) *Tuple {
	if row == nil {
		return nil
	}
	return &Tuple{*row}
}

func NewTupleWithSchema(values []*db_types.Value, schema Schema) (*Tuple, error) {
	if len(values) != len(schema.GetColumns()) {
		return nil, errors.New("schema column count is not equal to values' length")
	}

	tupleSize := 0
	unInlined := make([]Column, 0)
	for i, column := range schema.GetColumns() {
		if column.IsInlined() {
			tupleSize += values[i].Size()
		} else {
			unInlined = append(unInlined, column)
			tupleSize += UnInlinedColumnSize + values[i].Size()
		}
	}

	data := make([]byte, tupleSize)
	it := 0
	last := len(data)
	t := structures.Row{
		Data: nil,
		Rid:  structures.Rid{},
	}
	for i, column := range schema.GetColumns() {
		val := values[i]
		if column.IsInlined() {
			val.Serialize(data[it:])
			it += val.Size()
		} else {
			s := val.Size()
			binary.BigEndian.PutUint16(data[it:], uint16(s))
			binary.BigEndian.PutUint16(data[it+2:], uint16(last-s))
			val.Serialize(data[last-s:])
			it += UnInlinedColumnSize
			last -= s
		}
	}

	if it != last {
		panic("something went wrong")
	}

	t.Data = data
	return &Tuple{t}, nil
}
