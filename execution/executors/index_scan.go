package executors

import (
	"helin/btree"
	"helin/catalog"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/plans"
)

type IndexScanExecutor struct {
	BaseExecutor
	plan      *plans.IndexScanPlanNode
	indexIter *btree.TreeIterator
	index     *catalog.IndexInfo
}

func (e *IndexScanExecutor) Init() {
	e.index = e.executorCtx.Catalog.GetIndexByOID(e.plan.GetIndexOID())
}

func (e *IndexScanExecutor) GetOutSchema() catalog.Schema {
	return e.plan.OutSchema
}

func (e *IndexScanExecutor) Next(t *catalog.Tuple, rid *structures.Rid) error {
	panic("implement me")
}

func NewIndexScanExecutor(ctx *execution.ExecutorContext, plan *plans.IndexScanPlanNode) *IndexScanExecutor {
	return &IndexScanExecutor{
		BaseExecutor: BaseExecutor{
			executorCtx: ctx,
		},
		plan: plan,
	}
}
