package plans

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

type InsertPlanNode struct{
	BasePlanNode
	tableOID catalog.TableOID
	values [][]*db_types.Value
}

func (n *InsertPlanNode) GetType() PlanType{
	return Insert
}

func (n *InsertPlanNode) IsRawInsert() bool{
	return len(n.GetChildren()) == 0
}

func (n *InsertPlanNode) RawValuesAt(idx int) []*db_types.Value{
	return n.values[idx]
}

func (n *InsertPlanNode) RawValues() [][]*db_types.Value{
	return n.values
}

func (n *InsertPlanNode) GetTableOID() catalog.TableOID{
	return n.tableOID
}
 
// NewRawInsertPlanNode creates a new insert plan node for inserting raw values.
func NewRawInsertPlanNode(values [][]*db_types.Value, toid catalog.TableOID) *InsertPlanNode{
	return &InsertPlanNode{
		BasePlanNode: BasePlanNode{
			OutSchema: nil,
			Children:  []IPlanNode{},
		},
		tableOID:     toid,
		values:       values,
	}
}

func NewInsertPlanNode(child IPlanNode, toid catalog.TableOID) *InsertPlanNode{
	return &InsertPlanNode{
		BasePlanNode: BasePlanNode{
			OutSchema: nil,
			Children:  []IPlanNode{child},
		},
		tableOID:     toid,
		values:       [][]*db_types.Value{},
	}
}
