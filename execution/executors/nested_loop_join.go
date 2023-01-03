package executors

import (
	"errors"
	"helin/catalog"
	"helin/catalog/db_types"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/plans"
)

type NestedLoopJoinExecutor struct {
	BaseExecutor
	plan          *plans.NestedLoopJoinPlanNode
	leftExec      IExecutor
	rightExec     IExecutor
	lastLeftTuple *catalog.Tuple
}

func (e *NestedLoopJoinExecutor) Init() {
	e.leftExec.Init()
	e.rightExec.Init()
}

// GetOutSchema returns out schema of the plan if it is specified if not it returns concat of right and lefts
// plans' schema
func (e *NestedLoopJoinExecutor) GetOutSchema() catalog.Schema {
	if e.plan.GetOutSchema() == nil {
		rs, ls := e.plan.GetRightPlan().GetOutSchema(), e.plan.GetLeftPlan().GetOutSchema()
		return concatSchemas(ls, rs)
	}

	return e.plan.OutSchema
}

func (e *NestedLoopJoinExecutor) Next(t *catalog.Tuple, rid *structures.Rid) error {
	// TODO: what to set rid for joins or tuples that are not really persisted such as internal tables
	var rt, lt catalog.Tuple
	var rr, lr structures.Rid
	var rs, ls = e.plan.GetRightPlan().GetOutSchema(), e.plan.GetLeftPlan().GetOutSchema()
	if e.lastLeftTuple != nil {
		lt = *e.lastLeftTuple
	} else {
		if err := e.leftExec.Next(&lt, &lr); err != nil {
			return err
		}
		e.lastLeftTuple = &lt
	}

	for {
		var err error
		for err = e.rightExec.Next(&rt, &rr); err == nil; err = e.rightExec.Next(&rt, &rr) {
			val := e.plan.GetPredicate().EvalJoin(lt, ls, rt, rs)
			if !val.GetAsInterface().(bool) {
				continue
			}

			concat, err := concatTuple(lt, rt, ls, rs)
			if err != nil {
				return err
			}

			*t = *concat
			return nil
		}

		// if inner loop does not end with ErrNoTuple then it is actually an error. Stop and return the error.
		if errors.Is(err, ErrNoTuple{}) {
			e.rightExec.Init()
		} else {
			return err
		}

		if err := e.leftExec.Next(&lt, &lr); err != nil {
			return err
		}
		e.lastLeftTuple = &lt
	}
}

func NewNestedLoopJoinExecutor(ctx *execution.ExecutorContext, plan *plans.NestedLoopJoinPlanNode, l, r IExecutor) *NestedLoopJoinExecutor {
	return &NestedLoopJoinExecutor{
		BaseExecutor: BaseExecutor{executorCtx: ctx},
		plan:         plan,
		leftExec:     l,
		rightExec:    r,
	}
}

func concatSchemas(s1 catalog.Schema, s2 catalog.Schema) catalog.Schema {
	newColumns := make([]catalog.Column, 0, len(s1.GetColumns())+len(s2.GetColumns()))
	newColumns = append(newColumns, s1.GetColumns()...)

	for _, column := range s2.GetColumns() {
		newColumns = append(newColumns, catalog.Column{
			Name:   column.Name,
			TypeId: column.TypeId,
		})
	}

	return catalog.NewSchema(newColumns)
}

func concatTuple(t1, t2 catalog.Tuple, s1, s2 catalog.Schema) (*catalog.Tuple, error) {
	vals := make([]*db_types.Value, 0)
	for i := range s1.GetColumns() {
		vals = append(vals, t1.GetValue(s1, i))
	}

	for i := range s2.GetColumns() {
		vals = append(vals, t2.GetValue(s2, i))
	}

	return catalog.NewTupleWithSchema(vals, concatSchemas(s1, s2))
}
