package expressions

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

type GetColumnExpression struct {
	BaseExpression
	ColIdx   int
	TupleIdx int
}

func (e *GetColumnExpression) Eval(t catalog.Tuple, s catalog.Schema) db_types.Value {
	return *t.GetValue(s, e.ColIdx)
}

func (e *GetColumnExpression) EvalJoin(lt catalog.Tuple, ls catalog.Schema, rt catalog.Tuple, rs catalog.Schema) db_types.Value {
	if e.TupleIdx == 0 {
		return *lt.GetValue(ls, e.ColIdx)
	}

	return *rt.GetValue(rs, e.ColIdx)
}
