package plans

import (
	"helin/catalog"
	"helin/execution/expressions"
)

type IndexScanPlanNode struct {
	BasePlanNode
	predicate expressions.IExpression
	indexOID  catalog.IndexOID
}

func (n *IndexScanPlanNode) GetType() PlanType {
	return IndexScan
}

func (n *IndexScanPlanNode) GetPredicate() expressions.IExpression {
	return n.predicate
}

func (n *IndexScanPlanNode) GetIndexOID() catalog.IndexOID {
	return n.indexOID
}

func NewIndexScanPlanNode(outSchema catalog.Schema, pred expressions.IExpression, ioid catalog.IndexOID) *IndexScanPlanNode {
	return &IndexScanPlanNode{
		BasePlanNode: BasePlanNode{
			OutSchema: outSchema,
			Children:  []IPlanNode{},
		},
		predicate: pred,
		indexOID:  ioid,
	}
}
