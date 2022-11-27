package catalog

import (
	"helin/common"
)
import "fmt"
import strings2 "strings"

// TupleKey inherits from Tuple and relates tuple with a fixed sized schema so that
// it could be used as key in a b+ tree. It implements btree.Key interface. Less method
// defined on TupleKey compares corresponding columns and the most significant column is
// the first one in the schema. If left hand side TupleKey has less columns and all columns
// are equal lhs is considered less than rhs.
type TupleKey struct {
	Schema Schema
	Tuple
}

func (t *TupleKey) String() string {
	strings := make([]string, 0)
	for idx, _ := range t.Schema.GetColumns() {
		val := t.GetValue(t.Schema, idx)
		strings = append(strings, fmt.Sprintf("%v", val.GetAsInterface()))
	}
	return strings2.Join(strings, "-")
}

func (t *TupleKey) Less(than common.Key) bool {
	thanAsTupleKey := than.(*TupleKey)

	// find the longest schema, since one of them might be partial
	schema := t.Schema
	if len(t.Schema.GetColumns()) < len(thanAsTupleKey.Schema.GetColumns()) {
		schema = thanAsTupleKey.Schema
	}

	for idx, _ := range schema.GetColumns() {
		val1 := t.GetValue(schema, idx)
		val2 := thanAsTupleKey.GetValue(schema, idx)

		if val1 == nil || val2 == nil {
			if val1 == nil && val2 != nil {
				// if t is shorter than 'than' it is considered less.
				return true
			}
			return false
		}

		if val1.Less(val2) {
			return true
		} else if val2.Less(val1) {
			return false
		}
	}

	return false
}
