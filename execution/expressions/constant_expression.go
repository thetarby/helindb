package expressions

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

type ConstExpression struct{
	BaseExpression
	val db_types.Value
}

func (e *ConstExpression) Eval(t catalog.Tuple, s catalog.Schema) db_types.Value{
	return e.val
}