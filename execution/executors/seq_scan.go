package executors

import (
	"helin/catalog"
	"helin/disk/structures"
	"helin/execution"
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

func (e *SeqScanExecutor) Next(t *catalog.Tuple, rid *structures.Rid) error {
	it := e.tableIter
	for {
		row := it.Next()
		if row == nil {
			return ErrNoTuple{}
		}
	
		*t = *catalog.CastRowAsTuple(row)
		*rid = t.Rid
	
		pred := e.plan.GetPredicate()
		if pred != nil{
			val := pred.Eval(*t, e.GetOutSchema())
			if !val.GetAsInterface().(bool){
				continue
			}
		}
		
		return nil
	}
}

func NewSeqScanExecutor(ctx *execution.ExecutorContext,  plan *plans.SeqScanPlanNode) *SeqScanExecutor{
	return &SeqScanExecutor{
		BaseExecutor: BaseExecutor{
			executorCtx: ctx,
		},
		plan:         plan,
	}
}