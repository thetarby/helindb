package expressions

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

type CompType int

const (
	Equal CompType = iota
	NotEqual
	LessThan
	LessThanOrEqual
	GreaterThan
	GreaterThanOrEqual
)

type CompExpression struct{
	BaseExpression
	compType CompType
}

func (e *CompExpression) Eval(t catalog.Tuple, s catalog.Schema) db_types.Value{
	lhs := e.GetChildAt(0).Eval(t, s)
	rhs := e.GetChildAt(1).Eval(t, s)
	res := doComparison(e.compType, lhs, rhs)

	return *db_types.NewValue(res)
}

func doComparison(compType CompType, lhs, rhs db_types.Value) bool{
	switch compType{
	case Equal:
		if lhs.Less(&rhs){
			return false
		}else if rhs.Less(&lhs){
			return false
		}
		return true
	default:
		panic("implement me")
	}
}