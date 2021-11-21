package plans

import (
	"helin/catalog"
	"helin/execution/expressions"
)

type SeqScanPlanNode struct{
	BasePlanNode
	predicate expressions.IExpression
	tableOID catalog.TableOID
}

func (n *SeqScanPlanNode) GetType() PlanType{
	return SeqScan
}

func (n *SeqScanPlanNode) GetPredicate() expressions.IExpression{
	return n.predicate
}

func (n *SeqScanPlanNode) GetTableOID() catalog.TableOID{
	return n.tableOID
}
 