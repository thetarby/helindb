package expressions

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

type ConstExpression struct {
	BaseExpression
	Val db_types.Value
}

func (e *ConstExpression) Eval(t catalog.Tuple, s catalog.Schema) db_types.Value {
	return e.Val
}

func (e *ConstExpression) EvalJoin(lt catalog.Tuple, ls catalog.Schema, rt catalog.Tuple, rs catalog.Schema) db_types.Value {
	return e.Val
}
