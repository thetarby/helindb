package plans

import (
	"helin/catalog"
	"helin/execution/expressions"
)

type NestedLoopJoinPlanNode struct {
	BasePlanNode
	predicate expressions.IExpression
}

func (n *NestedLoopJoinPlanNode) GetType() PlanType {
	return NestedLoopJoin
}

func (n *NestedLoopJoinPlanNode) GetPredicate() expressions.IExpression {
	return n.predicate
}

func (n *NestedLoopJoinPlanNode) GetLeftPlan() IPlanNode {
	return n.GetChildAt(0)
}

func (n *NestedLoopJoinPlanNode) GetRightPlan() IPlanNode {
	return n.GetChildAt(1)
}

func NewNestedLoopJoinPlanNode(outSchema catalog.Schema, pred expressions.IExpression, left, right IPlanNode) *NestedLoopJoinPlanNode {
	return &NestedLoopJoinPlanNode{
		BasePlanNode: BasePlanNode{
			OutSchema: outSchema,
			Children:  []IPlanNode{left, right},
		},
		predicate: pred,
	}
}
