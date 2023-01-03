package catalog

import "helin/catalog/db_types"

const (
	UnInlinedColumnSize = 4
)

type Column struct {
	Name   string
	TypeId db_types.TypeID

	// Offset is the columns offset in the tuple
	Offset uint32
}

func (c *Column) IsInlined() bool {
	return c.TypeId.Size > 0
}

func (c *Column) InlinedSize() uint32 {
	if c.IsInlined() {
		return c.TypeId.Size
	}

	return UnInlinedColumnSize
}

func NewColumn(name string, typeId db_types.TypeID) Column {
	if typeId.Size == 0 {
		return Column{Name: name, TypeId: typeId}
	}

	return Column{Name: name, TypeId: typeId}
}
