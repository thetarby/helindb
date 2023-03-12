package executors

import (
	"fmt"
	"helin/buffer"
	"helin/catalog"
	"helin/catalog/db_types"
	"helin/common"
	"helin/disk/structures"
	"helin/execution"
	"helin/execution/plans"
	"helin/transaction"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func poolAndCatalog() (*buffer.BufferPool, catalog.Catalog, func()) {
	id, _ := uuid.NewUUID()
	dbName := id.String()

	poolSize := 1024
	pool := buffer.NewBufferPool(dbName, poolSize)
	ctg := catalog.NewCatalog(pool)

	return pool, ctg, func() { defer common.Remove(dbName) }
}

func TestInsertExecutor_Returns_ErrNoTuple_When_All_Is_Inserted(t *testing.T) {
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
	table := ctg.CreateTable(transaction.TxnNoop(), "myTable", schema)

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
		Txn:        nil,
		Catalog:    ctg,
		Pool:       pool,
		TxnManager: nil,
	}
	plan := plans.NewRawInsertPlanNode(rows, table.OID)

	// create and run executor
	exec := NewInsertExecutor(&ctx, plan, nil)
	exec.Init()

	var tup catalog.Tuple
	var rid structures.Rid
	for {
		if err := exec.Next(&tup, &rid); err != nil {
			require.ErrorIs(t, err, ErrNoTuple{})
			break
		}
	}
}
