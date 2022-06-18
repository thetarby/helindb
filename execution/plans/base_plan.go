package plans

import "helin/catalog"

type PlanType int

const (
	SeqScan PlanType = iota
	IndexScan
	IndexRangeScan
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

type IPlanNode interface {
	GetType() PlanType
	GetChildAt(idx int) IPlanNode
	GetChildren() []IPlanNode
	GetOutSchema() catalog.Schema
}

type BasePlanNode struct {
	/**
	 * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
	 * and this tells you what schema this plan node's tuples will have.
	 */
	OutSchema catalog.Schema
	Children  []IPlanNode
}

func (n *BasePlanNode) GetType() PlanType {
	panic("implement me")
}

func (n *BasePlanNode) GetChildAt(idx int) IPlanNode {
	return n.Children[idx]
}

func (n *BasePlanNode) GetChildren() []IPlanNode {
	return n.Children
}

func (n *BasePlanNode) GetOutSchema() catalog.Schema {
	return n.OutSchema
}
