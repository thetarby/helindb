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
	CompType CompType
}

func (e *CompExpression) Eval(t catalog.Tuple, s catalog.Schema) db_types.Value{
	lhs := e.GetChildAt(0).Eval(t, s)
	rhs := e.GetChildAt(1).Eval(t, s)
	res := doComparison(e.CompType, lhs, rhs)

	return *db_types.NewValue(res)
}

func (e *CompExpression) GetCompType(t catalog.Tuple, s catalog.Schema) CompType{
	return e.CompType
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
	case NotEqual:
		if lhs.Less(&rhs){
			return true
		}else if rhs.Less(&lhs){
			return true
		}
		return false
	case LessThan:
		return lhs.Less(&rhs)
	case LessThanOrEqual:
		return lhs.Less(&rhs) || !rhs.Less(&lhs)
	case GreaterThan:
		return rhs.Less(&lhs)
	case GreaterThanOrEqual:
		return rhs.Less(&rhs) || !lhs.Less(&rhs)
	default:
		panic("implement me")
	}
}

func NewCompExpr(cType CompType, lhs, rhs IExpression) *CompExpression{
	return &CompExpression{
		BaseExpression: BaseExpression{
			Children: []IExpression{lhs, rhs},
		},
		CompType:       cType,
	}
}
