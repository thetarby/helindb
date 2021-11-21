package catalog

import "helin/catalog/db_types"

type Column struct {
	Name   string
	TypeId db_types.TypeID

	// Offset is the columns offset in the tuple
	Offset uint16
}

// IsInlined returns true always for now
func (c *Column) IsInlined() bool {
	return true
}
