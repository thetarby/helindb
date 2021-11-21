package executors

import (
	"helin/catalog"
	"helin/disk/structures"
	"helin/execution/plans"
)

type SeqScanExecutor struct {
	BaseExecutor
	plan      *plans.SeqScanPlanNode
	tableIter *structures.TableIterator
}

func (e *SeqScanExecutor) Init() {
	table := e.executorCtx.Catalog.GetTableByOID(e.plan.GetTableOID())
	it := structures.NewTableIterator(e.executorCtx.Txn, table.Heap)
	e.tableIter = it
}

func (e *SeqScanExecutor) GetOutSchema() catalog.Schema {
	return e.plan.OutSchema
}

func (e *SeqScanExecutor) Next(t *catalog.Tuple, rid *structures.Rid) {
	it := e.tableIter
	t, rid = nil, nil
	row := it.Next()
	if row == nil {
		return
	}

	*t = *catalog.CastRowAsTuple(row)
	*rid = t.Rid
}
