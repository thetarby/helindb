package catalog

import (
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
	if col.IsInlined() {
		data := t.GetData()
		if int(col.Offset) >= len(data) {
			return nil
		}

		return db_types.Deserialize(col.TypeId, data[col.Offset:])
	}

	// not inlined columns are not implemented yet
	panic("implement me")
}

func (t *Tuple) GetRow() *structures.Row {
	return &t.Row
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
	data := make([]byte, 0)
	t := structures.Row{
		Data: nil,
		Rid:  structures.Rid{},
	}
	for i, column := range schema.GetColumns() {
		val := values[i]
		if column.IsInlined() {
			s := val.Size()
			temp := make([]byte, s)
			val.Serialize(temp)
			data = append(data, temp...)
		} else {
			panic("implement me")
		}
	}

	t.Data = data
	return &Tuple{t}, nil
}
