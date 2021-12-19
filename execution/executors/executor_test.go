package executors

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"helin/catalog"
	"helin/catalog/db_types"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/expressions"
	"helin/execution/plans"
	"io"
	"log"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeqScanExecutor_Equal_Comparison(t *testing.T) {
	pool, ctg, closer := poolAndCatalog()
	defer closer()

	// create a table in catalog
	columns := []catalog.Column{
		{
			Name:   "id",
			TypeId: db_types.IntegerTypeID,
			Offset: 0,
		},
		{
			Name:   "name",
			TypeId: db_types.CharTypeID,
			Offset: 4,
		},
		{
			Name:   "age",
			TypeId: db_types.IntegerTypeID,
			Offset: 24, // char type length it is fixed and 20. for now at least
		},
	}
	schema := catalog.NewSchema(columns)
	table := ctg.CreateTable("", "myTable", schema)

	// create raw values to insert
	n := 1000
	rows := make([][]*db_types.Value, 0)
	for i := 0; i < n; i++ {
		values := make([]*db_types.Value, 3) // 3 is number of columns
		values[0] = db_types.NewValue(int32(i))
		values[1] = db_types.NewValue(fmt.Sprintf("selam_%04d", i))
		values[2] = db_types.NewValue(int32(i % 20))
		rows = append(rows, values)
	}

	// create context and the plan
	ctx := execution.ExecutorContext{
		Txn:         nil,
		Catalog:     ctg,
		Pool:        pool,
		LockManager: nil,
		TxnManager:  nil,
	}
	plan := plans.NewRawInsertPlanNode(rows, table.OID)

	// create and run executor
	exec := NewInsertExecutor(&ctx, plan, nil)
	exec.Init()

	tup := catalog.Tuple{}
	rid := structures.Rid{}
	for {
		if err := exec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
	}

	pred := expressions.CompExpression{
		BaseExpression: expressions.BaseExpression{
			Children: []expressions.IExpression{
				&expressions.ConstExpression{
					BaseExpression: expressions.BaseExpression{Children: []expressions.IExpression{}},
					Val:            *db_types.NewValue("selam_0010"),
				},
				&expressions.GetColumnExpression{
					BaseExpression: expressions.BaseExpression{Children: []expressions.IExpression{}},
					ColIdx:         1,
				},
			},
		},
		CompType: expressions.Equal,
	}
	scanPlan := plans.NewSeqScanPlanNode(schema, &pred, table.OID)
	seqExec := NewSeqScanExecutor(&ctx, scanPlan)
	seqExec.Init()
	for {
		if err := seqExec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
		fmt.Println(tup.GetValue(schema, 1))
	}
}

func TestSeqScanExecutor_Greater_Than_Comparison(t *testing.T) {
	pool, ctg, closer := poolAndCatalog()
	defer closer()

	// create a table in catalog
	columns := []catalog.Column{
		{
			Name:   "id",
			TypeId: db_types.IntegerTypeID,
			Offset: 0,
		},
		{
			Name:   "name",
			TypeId: db_types.CharTypeID,
			Offset: 4,
		},
		{
			Name:   "age",
			TypeId: db_types.IntegerTypeID,
			Offset: 24, // char type length it is fixed and 20. for now at least
		},
	}
	schema := catalog.NewSchema(columns)
	table := ctg.CreateTable("", "myTable", schema)

	// create raw values to insert
	n := 1000
	rows := make([][]*db_types.Value, 0)
	for i := 0; i < n; i++ {
		values := make([]*db_types.Value, 3) // 3 is number of columns
		values[0] = db_types.NewValue(int32(i))
		values[1] = db_types.NewValue(fmt.Sprintf("selam_%04d", i))
		values[2] = db_types.NewValue(int32(i % 20))
		rows = append(rows, values)
	}

	// create context and the plan
	ctx := execution.ExecutorContext{
		Txn:         nil,
		Catalog:     ctg,
		Pool:        pool,
		LockManager: nil,
		TxnManager:  nil,
	}
	plan := plans.NewRawInsertPlanNode(rows, table.OID)

	// create and run executor
	exec := NewInsertExecutor(&ctx, plan, nil)
	exec.Init()

	tup := catalog.Tuple{}
	rid := structures.Rid{}
	for {
		if err := exec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
	}

	pred := expressions.CompExpression{
		BaseExpression: expressions.BaseExpression{
			Children: []expressions.IExpression{
				&expressions.ConstExpression{
					BaseExpression: expressions.BaseExpression{Children: []expressions.IExpression{}},
					Val:            *db_types.NewValue("selam_0010"),
				},
				&expressions.GetColumnExpression{
					BaseExpression: expressions.BaseExpression{Children: []expressions.IExpression{}},
					ColIdx:         1,
				},
			},
		},
		CompType: expressions.GreaterThanOrEqual,
	}
	scanPlan := plans.NewSeqScanPlanNode(schema, &pred, table.OID)
	seqExec := NewSeqScanExecutor(&ctx, scanPlan)
	seqExec.Init()
	for {
		if err := seqExec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
		fmt.Println(tup.GetValue(schema, 1))
	}
}

func TestIndexRangeScanExecutor(t *testing.T) {
	pool, ctg, closer := poolAndCatalog()
	defer closer()
	log.SetOutput(io.Discard)

	// create a table in catalog
	columns := []catalog.Column{
		{
			Name:   "id",
			TypeId: db_types.IntegerTypeID,
			Offset: 0,
		},
		{
			Name:   "name",
			TypeId: db_types.CharTypeID,
			Offset: 4,
		},
		{
			Name:   "age",
			TypeId: db_types.IntegerTypeID,
			Offset: 24, // char type length it is fixed and 20. for now at least
		},
	}
	tableSchema := catalog.NewSchema(columns)
	table := ctg.CreateTable("", "myTable", tableSchema)

	keyColumns := []catalog.Column{{
		Name:   "age",
		TypeId: db_types.IntegerTypeID,
		Offset: 0,
	}}
	keySchema := catalog.NewSchema(keyColumns)
	idx := ctg.CreateBtreeIndexWithTuple(nil, "idx", "myTable", keySchema, []int{2}, 4, false)

	// create raw values to insert
	n := 1000
	rows := make([][]*db_types.Value, 0)
	for i := 0; i < n; i++ {
		values := make([]*db_types.Value, 3) // 3 is number of columns
		age := int32(i % 20)
		values[0] = db_types.NewValue(int32(i))
		values[1] = db_types.NewValue(fmt.Sprintf("selam_%04d", age))
		values[2] = db_types.NewValue(age)
		rows = append(rows, values)
	}

	// create context and the plan
	ctx := execution.ExecutorContext{
		Txn:         nil,
		Catalog:     ctg,
		Pool:        pool,
		LockManager: nil,
		TxnManager:  nil,
	}
	plan := plans.NewRawInsertPlanNode(rows, table.OID)

	// create and run executor
	exec := NewInsertExecutor(&ctx, plan, nil)
	exec.Init()

	tup := catalog.Tuple{}
	rid := structures.Rid{}
	for {
		if err := exec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
	}

	min, max := db_types.NewValue(int32(10)), db_types.NewValue(int32(15))
	minTk := catalog.TupleKey{
		Schema: idx.Schema, // NOTE: keySchema is not equal to idx.Schema because idx is not a unique index and internally rid is appended to each key
		Tuple: catalog.Tuple{
			Row: structures.Row{
				Data: make([]byte, 4),
				Rid:  structures.Rid{},
			},
		},
	}
	maxTk := catalog.TupleKey{
		Schema: idx.Schema,
		Tuple: catalog.Tuple{
			Row: structures.Row{
				Data: make([]byte, 4),
				Rid:  structures.Rid{},
			},
		},
	}
	min.Serialize(minTk.Tuple.Data)
	max.Serialize(maxTk.Tuple.Data)

	scanPlan := plans.NewIndexRangeScanPlanNode(tableSchema, nil, &minTk, &maxTk, idx.OID)
	seqExec := NewIndexRangeScanExecutor(&ctx, scanPlan)
	seqExec.Init()
	readTuples := make([]catalog.Tuple, 0)
	for {
		if err := seqExec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
		readTuples = append(readTuples, tup)
	}
	assert.Len(t, readTuples, 250)
	assert.NotPanics(t, func() {
		for _, tup := range readTuples {
			x := tup.GetValue(tableSchema, 1)
			str := x.GetAsInterface().(string)
			if str > "selam_0015" || str < "selam_0010" {
				panic("")
			}
		}
	})
}
