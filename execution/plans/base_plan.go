package plans

import "helin/catalog"

type PlanType int

const (
	SeqScan PlanType = iota
	IndexScan
	Insert
	Update
	Delete
	Aggregation
	Limit
	Distinct
	NestedLoopJoin
	NestedIndexJoin
	HashJoin
)

type IPlanNode interface{
	GetType() PlanType
}

type BasePlanNode struct {
	/**
	 * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
	 * and this tells you what schema this plan node's tuples will have.
	 */
	OutSchema catalog.Schema
	Children  []*BasePlanNode
}

func (n *BasePlanNode) GetChildAt(idx int) *BasePlanNode{
	return n.Children[idx]
}

func (n *BasePlanNode) GetChildren() []*BasePlanNode{
	return n.Children
}

func (n *BasePlanNode) GetOutSchema() catalog.Schema{
	return n.OutSchema
}