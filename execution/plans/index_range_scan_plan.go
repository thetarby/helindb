package plans

import (
	"helin/catalog"
	"helin/common"
	"helin/execution/expressions"
)

type IndexRangeScanPlanNode struct {
	BasePlanNode
	indexOID catalog.IndexOID
	max      common.Key
	min      common.Key
}

func (n *IndexRangeScanPlanNode) GetType() PlanType {
	return IndexRangeScan
}

func (n *IndexRangeScanPlanNode) GetRange() (min common.Key, max common.Key) {
	return n.min, n.max
}

func (n *IndexRangeScanPlanNode) GetIndexOID() catalog.IndexOID {
	return n.indexOID
}

func NewIndexRangeScanPlanNode(outSchema catalog.Schema, pred expressions.IExpression, min, max common.Key, ioid catalog.IndexOID) *IndexRangeScanPlanNode {
	return &IndexRangeScanPlanNode{
		BasePlanNode: BasePlanNode{
			OutSchema: outSchema,
			Children:  []IPlanNode{},
		},
		indexOID: ioid,
		max:      max,
		min:      min,
	}
}
