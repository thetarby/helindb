package executors

import (
	"helin/catalog"
	"helin/disk/structures"
	"helin/execution"
)

type IExecutor interface {
	Init()

	// Next yields next tuple from executor
	Next(t *catalog.Tuple, rid *structures.Rid) error

	GetExecutorCtx() *execution.ExecutorContext

	// GetOutSchema returns the schema of the yielded tuples
	GetOutSchema() catalog.Schema
}

type BaseExecutor struct {
	executorCtx *execution.ExecutorContext
}

func (e *BaseExecutor) GetExecutorCtx() *execution.ExecutorContext{
	return e.executorCtx
}

func (e *BaseExecutor) Init(){
	panic("implement me")
}

func (e *BaseExecutor) Next(t *catalog.Tuple, rid *structures.Rid) error{
	panic("implement me")
}

func (e *BaseExecutor) GetOutSchema() catalog.Schema{
	panic("implement me")
}
