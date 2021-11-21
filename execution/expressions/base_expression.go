package expressions

import (
	"helin/catalog"
	"helin/catalog/db_types"
)

// IExpression is the node in expression tree
type IExpression interface{
	Eval(catalog.Tuple, catalog.Schema) db_types.Value
	GetChildAt(idx int) IExpression
	GetChildren() []IExpression
}

// BaseExpression implements trivial methods needed for each type implementing IExpression interface such as 
// tree traversal methods
type BaseExpression struct{
	Children []IExpression
}

func (e *BaseExpression) Eval(catalog.Tuple, catalog.Schema) db_types.Value{
	panic("implement me")
}

func (e *BaseExpression) GetChildAt(idx int) IExpression{
	return e.Children[idx]
}

func (e *BaseExpression) GetChildren() []IExpression{
	return e.Children
}