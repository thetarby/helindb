package catalog

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/buffer"
	"helin/catalog/db_types"
	"helin/disk/structures"
	"io"
	"log"
	"os"
	"testing"
)

func TestCatalog_CreateTable_Should_Create_Table_Successfully(t *testing.T) {
	log.SetOutput(io.Discard)
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 32)

	catalog := NewCatalog(pool)
	schema := SchemaImpl{
		columns: []Column{
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
		},
	}
	table := catalog.CreateTable("", "myTable", &schema)

	assert.Equal(t, *table, *catalog.GetTable("myTable"))
}

func TestCatalog(t *testing.T) {
	log.SetOutput(io.Discard)
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 32)

	catalog := NewCatalog(pool)
	schema := SchemaImpl{
		columns: []Column{
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
		},
	}
	table := catalog.CreateTable("", "myTable", &schema)

	n := 10
	rids := make([]structures.Rid, n)
	for i := 0; i < n; i++ {
		values := make([]*db_types.Value, 2) // 2 is number of columns
		values[0] = db_types.NewValue(int32(i))
		values[1] = db_types.NewValue("selam")

		tuple, err := NewTupleWithSchema(values, &schema)
		require.NoError(t, err)
		rid, err := table.Heap.InsertTuple(tuple.Row, "")
		require.NoError(t, err)
		rids[i] = rid
	}

	for i, rid := range rids {
		dest := structures.Row{}
		err := table.Heap.ReadTuple(rid, &dest, "")
		require.NoError(t, err)
		tuple := CastRowAsTuple(&dest)
		intVal := tuple.GetValue(&schema, 0)
		strVal := tuple.GetValue(&schema, 1)

		assert.Equal(t, db_types.IntegerTypeID, intVal.GetTypeId())
		assert.Equal(t, db_types.CharTypeID, strVal.GetTypeId())
		assert.Equal(t, int32(i), intVal.GetAsInterface().(int32))
		assert.Equal(t, "selam", strVal.GetAsInterface().(string))
	}
}
