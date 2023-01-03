package catalog

import "errors"

type Schema interface {
	GetColumns() []Column
	GetColumn(idx int) *Column
	GetColIdx(name string) (int, error)
}

type SchemaImpl struct {
	columns []Column
}

func (s *SchemaImpl) GetColIdx(name string) (int, error) {
	for i, column := range s.columns {
		if column.Name == name {
			return i, nil
		}
	}

	return 0, errors.New("columns does not exist")
}

func (s *SchemaImpl) GetColumns() []Column {
	return s.columns
}

func (s *SchemaImpl) GetColumn(idx int) *Column {
	return &s.columns[idx]
}

func NewSchema(cols []Column) Schema {
	// set offsets of each column
	var offset uint32 = 0
	for i := 0; i < len(cols); i++ {
		cols[i].Offset = offset
		offset += cols[i].InlinedSize()
	}

	return &SchemaImpl{
		columns: cols,
	}
}
