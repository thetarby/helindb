package catalog

type Column struct {
	Name   string
	TypeId uint8

	// Offset is the columns offset in the tuple
	Offset uint16
}

// IsInlined returns true always for now
func (c *Column) IsInlined() bool {
	return true
}
