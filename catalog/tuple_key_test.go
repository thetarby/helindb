package catalog

import (
	"helin/btree"
	"helin/buffer"
	"helin/catalog/db_types"
	"helin/common"
	"helin/disk/structures"
	"io"
	"log"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCatalog_Create_Index_On_Unpopulated_Table_2_Nonunique_Index(t *testing.T) {
	log.SetOutput(io.Discard)
	dbName := "db.helin"
	defer os.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 32)

	keyColumns := []Column{
		{
			Name:   "age",
			TypeId: db_types.IntegerTypeID,
			Offset: 0,
		},
		{
			Name:   "id",
			TypeId: db_types.IntegerTypeID,
			Offset: 4,
		},
	}
	keySchema := NewSchema(keyColumns)
	serializer := TupleKeySerializer{schema: keySchema}
	index := btree.NewBtreeWithPager(50, btree.NewDefaultBPP(pool, &serializer))

	ageToFind := 16
	toFind := make([]int, 0)
	n := 10_000
	for i := 0; i < n; i++ {
		values := make([]*db_types.Value, 2) // 2 is number of columns
		age := (int32(i) % 20)
		values[0] = db_types.NewValue(age)
		values[1] = db_types.NewValue(int32(i))
		tuple, err := NewTupleWithSchema(values, keySchema)
		require.NoError(t, err)
		tk := TupleKey{
			Schema: keySchema,
			Tuple:  *tuple,
		}

		sp := btree.SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(age),
		}
		index.Insert(&tk, sp)
		if age >= int32(ageToFind) {
			toFind = append(toFind, int(sp.PageId))
		}
	}

	// find in index
	val := db_types.NewValue(int32(ageToFind))
	dest := make([]byte, val.Size())
	val.Serialize(dest)
	tkToFind := TupleKey{
		Schema: keySchema,
		Tuple: Tuple{
			Row: structures.Row{
				Data: dest,
				Rid:  structures.Rid{},
			},
		},
	}

	for _, x := range index.FindSince(&tkToFind) {
		sp := x.(btree.SlotPointer)
		i := common.IndexOfInt(int(sp.PageId), toFind)
		if !assert.NotEqual(t, -1, i) {
			println(sp.PageId)
			return
		}
		if len(toFind) == 1 {
			toFind = nil
		} else {
			copy(toFind[i:], toFind[i+1:])
			toFind = toFind[:len(toFind)-1]
		}
		require.True(t, sp.SlotIdx >= int16(ageToFind))
	}

	assert.Empty(t, toFind)
}
