package expressions

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

type GetColumnExpression struct{
	BaseExpression
	ColIdx int
}

func (e *GetColumnExpression) Eval(t catalog.Tuple, s catalog.Schema) db_types.Value{
	return *t.GetValue(s, e.ColIdx)
}