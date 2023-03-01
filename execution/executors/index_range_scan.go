package executors

import (
	"helin/btree"
	"helin/catalog"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/plans"
)

type IndexRangeScanExecutor struct {
	BaseExecutor
	plan      *plans.IndexRangeScanPlanNode
	indexIter *btree.TreeIterator
	index     *catalog.IndexInfo
}

func (e *IndexRangeScanExecutor) Init() {
	min, _ := e.plan.GetRange()
	e.index = e.executorCtx.Catalog.GetIndexByOID(e.plan.GetIndexOID())
	if min != nil {
		e.indexIter = btree.NewTreeIteratorWithKey(e.executorCtx.Txn, min, e.index.Index, e.index.Index.GetPager())
	} else {
		e.indexIter = btree.NewTreeIterator(e.executorCtx.Txn, e.index.Index)
	}
}

func (e *IndexRangeScanExecutor) GetOutSchema() catalog.Schema {
	return e.plan.OutSchema
}

func (e *IndexRangeScanExecutor) Next(t *catalog.Tuple, rid *structures.Rid) error {
	_, max := e.plan.GetRange()
	for key, val := e.indexIter.Next(); val != nil; key, val = e.indexIter.Next() {
		if key.Less(max) {
			valAsRid := structures.Rid(val.(btree.SlotPointer))
			e.index.GetTable().Heap.ReadTuple(valAsRid, t.GetRow(), e.executorCtx.Txn)
			return nil
		}
		return ErrNoTuple{}
	}

	return ErrNoTuple{}
}

func NewIndexRangeScanExecutor(ctx *execution.ExecutorContext, plan *plans.IndexRangeScanPlanNode) *IndexRangeScanExecutor {
	return &IndexRangeScanExecutor{
		BaseExecutor: BaseExecutor{
			executorCtx: ctx,
		},
		plan: plan,
	}
}
