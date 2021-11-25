package executors

import (
	"fmt"
	"helin/catalog"
	"helin/catalog/db_types"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/expressions"
	"helin/execution/plans"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeqScanExecutor(t *testing.T) {
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
		if err := exec.Next(&tup, &rid); err != nil{
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
	}

	pred := expressions.CompExpression{
		BaseExpression: expressions.BaseExpression{
			Children: []expressions.IExpression{
				&expressions.ConstExpression{
					BaseExpression: expressions.BaseExpression{	Children: []expressions.IExpression{}},
					Val: *db_types.NewValue("selam_0010"),
				},
				&expressions.GetColumnExpression{
					BaseExpression: expressions.BaseExpression{Children: []expressions.IExpression{}},
					ColIdx:         1,
				},
			},
		},
	}
	scanPlan := plans.NewSeqScanPlanNode(schema, &pred, table.OID)
	seqExec := NewSeqScanExecutor(&ctx, scanPlan)
	seqExec.Init()
	for {
		if err := seqExec.Next(&tup, &rid); err != nil{
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
		fmt.Println(tup.GetValue(schema, 1))
	}
}