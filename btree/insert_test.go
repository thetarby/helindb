package btree

import (
	"github.com/stretchr/testify/require"
	"helin/buffer"
	"helin/common"
	"helin/disk"
	"helin/disk/wal"
	"helin/transaction"
	"io"
	"log"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestInsert_Should_Split_Root_When_It_Has_M_Keys(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	tree.Insert(transaction.TxnNoop(), PersistentKey(1), "1")
	tree.Insert(transaction.TxnNoop(), PersistentKey(5), "5")
	tree.Insert(transaction.TxnNoop(), PersistentKey(3), "3")

	res, stack := tree.FindAndGetStack(PersistentKey(5), Read)

	assert.Len(t, stack, 2)
	assert.Equal(t, "5", res)
	assert.Equal(t, PersistentKey(3), tree.GetRoot(Read).GetKeyAt(0))
}

func TestInsert_Or_Replace_Should_Return_False_When_Key_Exists(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for i := 0; i < 1000; i++ {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), strconv.Itoa(i))
	}

	isInserted := tree.InsertOrReplace(transaction.TxnNoop(), PersistentKey(500), "new_500")

	assert.False(t, isInserted)
}

func TestInsert_Or_Replace_Should_Replace_Value_When_Key_Exists(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for i := 0; i < 1000; i++ {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), strconv.Itoa(i))
	}

	tree.InsertOrReplace(transaction.TxnNoop(), PersistentKey(500), "new_500")
	val := tree.Find(PersistentKey(500))

	assert.Contains(t, val.(string), "new_500")
}

func TestAll_Inserts_Should_Be_Found_By_Find_Method(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	dm, _, err := disk.NewDiskManager(dbName)
	require.NoError(t, err)
	defer common.Remove(dbName)

	lm := wal.NewLogManager(dm.GetLogWriter())
	pool := buffer.NewBufferPoolWithDM(1024, dm, lm)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewBPP(pool, &PersistentKeySerializer{}, &StringValueSerializer{}, lm))
	log.SetOutput(io.Discard)

	arr := make([]int, 0)
	for i := 0; i < 1000; i++ {
		arr = append(arr, i)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(arr), func(i, j int) { arr[i], arr[j] = arr[j], arr[i] })

	for _, item := range arr {
		tree.Insert(transaction.TxnNoop(), PersistentKey(item), strconv.Itoa(item))
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(arr), func(i, j int) { arr[i], arr[j] = arr[j], arr[i] })
	for _, item := range arr {
		val := tree.Find(PersistentKey(item))
		assert.NotNil(t, val)
		assert.Equal(t, strconv.Itoa(item), val.(string))
	}
}

func TestInsert_Internals(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 4, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	tree.Insert(transaction.TxnNoop(), PersistentKey(1), "1")

	val, stack := tree.FindAndGetStack(PersistentKey(1), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "1", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(2), "2")
	val, stack = tree.FindAndGetStack(PersistentKey(2), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "2", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(3), "3")
	val, stack = tree.FindAndGetStack(PersistentKey(3), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "3", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(4), "4")
	val, stack = tree.FindAndGetStack(PersistentKey(4), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "4", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(5), "5")
	val, stack = tree.FindAndGetStack(PersistentKey(5), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "5", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(6), "6")
	val, stack = tree.FindAndGetStack(PersistentKey(6), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "6", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(7), "7")
	val, stack = tree.FindAndGetStack(PersistentKey(7), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "7", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(8), "8")
	val, stack = tree.FindAndGetStack(PersistentKey(8), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "8", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(9), "9")
	val, stack = tree.FindAndGetStack(PersistentKey(9), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, "9", val.(string))

	tree.Insert(transaction.TxnNoop(), PersistentKey(10), "10")
	val, stack = tree.FindAndGetStack(PersistentKey(10), Read)
	tree.runlatch(stack)

	assert.Len(t, stack, 3)
	assert.Equal(t, "10", val.(string))
}

func TestInsert_Internals_2(t *testing.T) {
	log.SetOutput(io.Discard)
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 8)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewDefaultBPP(pool, &PersistentKeySerializer{}, io.Discard))

	n := 10000
	for i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
	}

	_, stack := tree.FindAndGetStack(PersistentKey(9000), Read)
	leftMostNode := stack[len(stack)-1].Node
	for {
		if leftMostNode == nil {
			break
		}
		old := leftMostNode
		leftMostNode = tree.pager.GetNode(leftMostNode.GetRight(), Read)
		tree.pager.Unpin(old, false)
	}

	assert.True(t, true)
}
