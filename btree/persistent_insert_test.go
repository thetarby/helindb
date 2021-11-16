package btree

import (
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"helin/buffer"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

func TestPersistent_Insert_Should_Split_Root_When_It_Has_M_Keys(t *testing.T) {
	tree := NewBtreeWithPager(3, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, KeySize: 8})
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	tree.Insert(PersistentKey(1), p)
	tree.Insert(PersistentKey(5), p)
	tree.Insert(PersistentKey(3), p)

	var stack []NodeIndexPair

	res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(5), stack)

	assert.Len(t, stack, 2)
	assert.Equal(t, p, res.(SlotPointer))
	assert.Equal(t, PersistentKey(3), tree.pager.GetNode(tree.Root).GetKeyAt(0))
}

func TestPersistentInsert_Or_Replace_Should_Return_False_When_Key_Exists(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 8)
	tree := NewBtreeWithPager(80, NewBufferPoolPager(pool, &PersistentKeySerializer{}, 8))
	for i := 0; i < 1000; i++ {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}

	isInserted := tree.InsertOrReplace(PersistentKey(500), SlotPointer{
		PageId:  int64(1500),
		SlotIdx: int16(1500),
	})

	assert.False(t, isInserted)
}

func TestPersistentEvery_Inserted_Should_Be_Found(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)
	tree := NewBtreeWithPager(10, NewBufferPoolPager(pool, &PersistentKeySerializer{}, 8))
	log.SetOutput(ioutil.Discard)
	n := 1000
	for _, i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
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
			PageId:  int64(i),
			SlotIdx: int16(i),
		}, val.(SlotPointer))
	}
	//fmt.Println(buffer.Victims)
	//fmt.Println(buffer.Accessed)
}

func TestPersistent_Pin_Count_Should_Be_Zero_After_Inserts_Are_Complete(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 4)
	tree := NewBtreeWithPager(10, NewBufferPoolPager(pool, &PersistentKeySerializer{}, 8))
	log.SetOutput(ioutil.Discard)
	n := 1000
	for _, i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}

	assert.Equal(t, 0, pool.Replacer.NumPinnedPages())
}

func TestPersistentInsert_Or_Replace_Should_Replace_Value_When_Key_Exists(t *testing.T) {
	tree := NewBtree(3)
	for i := 0; i < 1000; i++ {
		tree.Insert(MyInt(i), strconv.Itoa(i))
	}

	tree.InsertOrReplace(MyInt(500), "new_500")
	val := tree.Find(MyInt(500))

	assert.Equal(t, "new_500", val.(string))
}

func TestPersistent_All_Inserted_Should_Be_Found_After_File_Is_Closed_And_Reopened(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	poolSize := 64
	pool := buffer.NewBufferPool(dbName, poolSize)
	tree := NewBtreeWithPager(80, NewBufferPoolPager(pool, &PersistentKeySerializer{}, 8))
	log.SetOutput(ioutil.Discard)

	n := 10_000
	v := make([]PersistentKey, n, n)
	for i := 0; i < n; i++ {
		v[i] = PersistentKey(rand.Intn(math.MaxInt32))
	}

	for _, i := range v {
		tree.Insert(i, SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}

	tree.pager.(*BufferPoolPager).pool.FlushAll()
	err := tree.pager.(*BufferPoolPager).pool.DiskManager.Close()
	assert.NoError(t, err)
	newPool := buffer.NewBufferPool(dbName, poolSize)
	newTreeReference := ConstructBtreeFromRootPointer(tree.Root, 80, NewBufferPoolPager(newPool, &PersistentKeySerializer{}, 8))

	rand.Shuffle(len(v), func(i, j int) {
		t := v[i]
		v[i] = v[j]
		v[j] = t
	})
	for _, i := range v {
		val := newTreeReference.Find(i)
		assert.Equal(t, SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		}, val.(SlotPointer))
	}

}

func TestPersistent_Find_Should_Unpin_All_Nodes_It_Pinned(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 4)
	tree := NewBtreeWithPager(10, NewBufferPoolPager(pool, &PersistentKeySerializer{}, 8))
	log.SetOutput(ioutil.Discard)
	n := 1000
	for _, i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}

	val := tree.Find(PersistentKey(999))
	assert.NotNil(t, val)
	assert.Zero(t, pool.Replacer.NumPinnedPages())
}