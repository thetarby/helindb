package executors

import (
	"errors"
	"helin/catalog"
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

			nr := concatRows(lt.Row, rt.Row)
			nt := catalog.Tuple{Row: nr}
			*t = nt
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

	last := s1.GetColumns()[len(s1.GetColumns())-1]

	for _, column := range s2.GetColumns() {
		newColumns = append(newColumns, catalog.Column{
			Name:   column.Name,
			TypeId: column.TypeId,
			Offset: column.Offset + last.Offset + uint16(last.TypeId.Size),
		})
	}

	return catalog.NewSchema(newColumns)
}

func concatRows(r1 structures.Row, r2 structures.Row) structures.Row {
	d1, d2 := r1.GetData(), r2.GetData()
	newData := make([]byte, 0, len(d1)+len(d2))
	newData = append(newData, d1...)
	newData = append(newData, d2...)

	return structures.Row{
		Data: newData,
		Rid:  structures.Rid{},
	}
}
