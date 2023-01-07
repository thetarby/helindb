package btree

import (
	"fmt"
	"helin/buffer"
	"helin/common"
	"helin/transaction"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestPersistent_Insert_Should_Split_Root_When_It_Has_M_Keys(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewMemPager(&PersistentKeySerializer{}, &SlotPointerValueSerializer{}))
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	tree.Insert(transaction.TxnNoop(), PersistentKey(1), p)
	tree.Insert(transaction.TxnNoop(), PersistentKey(5), p)
	tree.Insert(transaction.TxnNoop(), PersistentKey(3), p)

	res, stack := tree.FindAndGetStack(PersistentKey(5), Read)
	assert.Len(t, stack, 2)
	assert.Equal(t, p, res.(SlotPointer))
	assert.Equal(t, PersistentKey(3), tree.GetRoot(Read).GetKeyAt(0))
}

func TestPersistentInsert_Or_Replace_Should_Return_False_When_Key_Exists(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 8)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 80, NewDefaultBPP(pool, &PersistentKeySerializer{}, io.Discard))
	for i := 0; i < 1000; i++ {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
	}

	isInserted := tree.InsertOrReplace(transaction.TxnNoop(), PersistentKey(500), SlotPointer{
		PageId:  uint64(1500),
		SlotIdx: int16(1500),
	})

	assert.False(t, isInserted)
}

func TestPersistentEvery_Inserted_Should_Be_Found(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewDefaultBPP(pool, &PersistentKeySerializer{}, io.Discard))
	log.SetOutput(ioutil.Discard)
	n := 100000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
	}

	for i := 0; i < n; i++ {
		val := tree.Find(PersistentKey(i))
		if val == nil {
			//tree.Print()
			val = tree.Find(PersistentKey(i))
		}
		assert.Equal(t, SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		}, val.(SlotPointer))
	}
	//fmt.Println(buffer.Victims)
	//fmt.Println(buffer.Accessed)
}

func TestPersistentEvery_Inserted_Should_Be_Found_VarSized(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 16)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 32, NewBPP(pool, &StringKeySerializer{}, &StringValueSerializer{}, nil))
	log.SetOutput(ioutil.Discard)
	n := 100000
	for _, i := range rand.Perm(n) {
		key, val := fmt.Sprintf("sa_%v", i), fmt.Sprintf("as_%v", i)
		tree.Insert(transaction.TxnNoop(), StringKey(key), val)
	}

	for i := 0; i < n; i++ {
		val := tree.Find(StringKey(fmt.Sprintf("sa_%v", i)))

		assert.Equal(t, fmt.Sprintf("as_%v", i), val.(string))
	}
}

func TestPersistent_Pin_Count_Should_Be_Zero_After_Inserts_Are_Complete(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 5)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewDefaultBPP(pool, &PersistentKeySerializer{}, io.Discard))
	log.SetOutput(ioutil.Discard)
	n := 1000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
		if pool.Replacer.NumPinnedPages() != 0 {
			t.Error("# of pinned pages is not 0")
		}
	}

	assert.Equal(t, 0, pool.Replacer.NumPinnedPages())
}

func TestPersistentInsert_Or_Replace_Should_Replace_Value_When_Key_Exists(t *testing.T) {
	tree := NewBtreeWithPager(transaction.TxnNoop(), 3, NewMemPager(&PersistentKeySerializer{}, &StringValueSerializer{}))
	for i := 0; i < 1000; i++ {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), strconv.Itoa(i))
	}

	tree.InsertOrReplace(transaction.TxnNoop(), PersistentKey(500), "new_500")
	val := tree.Find(PersistentKey(500))

	assert.Contains(t, val.(string), "new_500")
}

func TestPersistent_All_Inserted_Should_Be_Found_After_File_Is_Closed_And_Reopened(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	poolSize := 64
	pool := buffer.NewBufferPool(dbName, poolSize)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 80, NewDefaultBPP(pool, &PersistentKeySerializer{}, io.Discard))
	log.SetOutput(ioutil.Discard)

	n := 10_000
	v := make([]PersistentKey, n, n)
	for i := 0; i < n; i++ {
		v[i] = PersistentKey(rand.Intn(math.MaxInt32))
	}

	for _, i := range v {
		tree.Insert(transaction.TxnNoop(), i, SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
	}

	tree.pager.(*BufferPoolPager).pool.FlushAll()
	err := tree.pager.(*BufferPoolPager).pool.DiskManager.Close()
	assert.NoError(t, err)
	newPool := buffer.NewBufferPool(dbName, poolSize)
	newTreeReference := ConstructBtreeByMeta(tree.metaPID, NewDefaultBPP(newPool, &PersistentKeySerializer{}, io.Discard))

	rand.Shuffle(len(v), func(i, j int) {
		t := v[i]
		v[i] = v[j]
		v[j] = t
	})
	for _, i := range v {
		val := newTreeReference.Find(i)
		assert.Equal(t, SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		}, val.(SlotPointer))
	}

}

func TestPersistent_Find_Should_Unpin_All_Nodes_It_Pinned(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 5)
	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewDefaultBPP(pool, &PersistentKeySerializer{}, io.Discard))
	log.SetOutput(ioutil.Discard)
	n := 1000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), PersistentKey(i), SlotPointer{
			PageId:  uint64(i),
			SlotIdx: int16(i),
		})
		if pool.Replacer.NumPinnedPages() != 0 {
			t.Error("# of pinned pages is not 0")
		}
	}

	val := tree.Find(PersistentKey(999))
	assert.NotNil(t, val)
	assert.Zero(t, pool.Replacer.NumPinnedPages())
}
