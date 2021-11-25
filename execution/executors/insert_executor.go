package executors

import (
	"helin/catalog"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/plans"
)

type InsertExecutor struct {
	BaseExecutor
	plan      *plans.InsertPlanNode
	childExecutor IExecutor
	lastInsertedRawValue int
}

func (e *InsertExecutor) Init() {
	e.lastInsertedRawValue = -1
	if !e.plan.IsRawInsert(){
		e.childExecutor.Init()
	}
}

func (e *InsertExecutor) GetOutSchema() catalog.Schema {
	return e.plan.OutSchema
}

func (e *InsertExecutor) Next(t *catalog.Tuple, rid *structures.Rid) error {
	// TODO: validate schemas 
	tOID := e.plan.GetTableOID()
	table := e.executorCtx.Catalog.GetTableByOID(tOID)
	if e.plan.IsRawInsert(){
		e.lastInsertedRawValue++

		if e.lastInsertedRawValue == len(e.plan.RawValues()){
			return ErrNoTuple{}
		}

		table.InsertTupleViaValues(e.plan.RawValuesAt(e.lastInsertedRawValue), e.executorCtx.Txn)
		return nil
	}else{
		if err := e.childExecutor.Next(t, rid); err  != nil{
			return err
		}
	}
	table.InsertTuple(t, e.executorCtx.Txn)

	e.GetOutSchema().GetColumns()
	
	return nil
}

func NewInsertExecutor(ctx *execution.ExecutorContext, plan *plans.InsertPlanNode, childExecutor IExecutor) *InsertExecutor{
	return &InsertExecutor{
		BaseExecutor:         BaseExecutor{
			executorCtx: ctx,
		},
		plan:                 plan,
		childExecutor:        childExecutor,
		lastInsertedRawValue: -1,
	}
}