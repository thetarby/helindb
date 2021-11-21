package btree

import (
	"helin/buffer"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDemo(t *testing.T) {
	got := 4 + 6
	want := 10

	if got != want {
		t.Errorf("got %q, wanted %q", got, want)
	}
}

func padStr(str string) string {
	res := make([]byte, 10)
	for i, v := range []byte(str) {
		res[i] = v
	}

	return string(res)
}

func TestInsert_Should_Split_Root_When_It_Has_M_Keys(t *testing.T) {
	tree := NewBtreeWithPager(3, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, ValueSerializer: &StringValueSerializer{Len: 10}})
	tree.Insert(PersistentKey(1), padStr("1"))
	tree.Insert(PersistentKey(5), padStr("5"))
	tree.Insert(PersistentKey(3), padStr("3"))

	var stack []NodeIndexPair

	res, stack := tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(5), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("5"), res)
	assert.Equal(t, PersistentKey(3), tree.pager.GetNode(tree.Root).GetKeyAt(0))
}

func TestInsert_Or_Replace_Should_Return_False_When_Key_Exists(t *testing.T) {
	tree := NewBtreeWithPager(3, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, ValueSerializer: &StringValueSerializer{Len: 10}})
	for i := 0; i < 1000; i++ {
		tree.Insert(PersistentKey(i), strconv.Itoa(i))
	}

	isInserted := tree.InsertOrReplace(PersistentKey(500), "new_500")

	assert.False(t, isInserted)
}

func TestInsert_Or_Replace_Should_Replace_Value_When_Key_Exists(t *testing.T) {
	tree := NewBtreeWithPager(3, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, ValueSerializer: &StringValueSerializer{Len: 10}})
	for i := 0; i < 1000; i++ {
		tree.Insert(PersistentKey(i), strconv.Itoa(i))
	}

	tree.InsertOrReplace(PersistentKey(500), "new_500")
	val := tree.Find(PersistentKey(500))

	assert.Contains(t, val.(string), "new_500")
}

func TestAll_Inserts_Should_Be_Found_By_Find_Method(t *testing.T) {
	tree := NewBtreeWithPager(3, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, ValueSerializer: &StringValueSerializer{Len: 10}})
	arr := make([]int, 0)
	for i := 0; i < 1000; i++ {
		arr = append(arr, i)
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(arr), func(i, j int) { arr[i], arr[j] = arr[j], arr[i] })

	for _, item := range arr {
		tree.Insert(PersistentKey(item), strconv.Itoa(item))
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(arr), func(i, j int) { arr[i], arr[j] = arr[j], arr[i] })
	for _, item := range arr {
		val := tree.Find(PersistentKey(item))
		assert.NotNil(t, val)
		assert.Equal(t, padStr(strconv.Itoa(item)), val.(string))
	}
}

func TestInsert_Internals(t *testing.T) {
	tree := NewBtreeWithPager(4, NoopPersistentPager{KeySerializer: &PersistentKeySerializer{}, ValueSerializer: &StringValueSerializer{Len: 10}})
	tree.Insert(PersistentKey(1), "1")

	stack := make([]NodeIndexPair, 0)
	val, stack := tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(1), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("1"), val.(string))

	tree.Insert(PersistentKey(2), "2")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(2), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("2"), val.(string))

	tree.Insert(PersistentKey(3), "3")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(3), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("3"), val.(string))

	tree.Insert(PersistentKey(4), "4")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(4), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("4"), val.(string))

	tree.Insert(PersistentKey(5), "5")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(5), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("5"), val.(string))

	tree.Insert(PersistentKey(6), "6")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(6), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("6"), val.(string))

	tree.Insert(PersistentKey(7), "7")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(7), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("7"), val.(string))

	tree.Insert(PersistentKey(8), "8")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(8), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("8"), val.(string))

	tree.Insert(PersistentKey(9), "9")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(9), stack, Insert)

	assert.Len(t, stack, 2)
	assert.Equal(t, padStr("9"), val.(string))

	tree.Insert(PersistentKey(10), "10")
	stack = make([]NodeIndexPair, 0)
	val, stack = tree.pager.GetNode(tree.Root).findAndGetStack(PersistentKey(10), stack, Insert)

	assert.Len(t, stack, 3)
	assert.Equal(t, padStr("10"), val.(string))
}

func TestInsert_Internals_2(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)
	pool := buffer.NewBufferPool(dbName, 8)
	tree := NewBtreeWithPager(10, NewBufferPoolPager(pool, &PersistentKeySerializer{}))

	n := 10000
	for i := range rand.Perm(n) {
		tree.Insert(PersistentKey(i), SlotPointer{
			PageId:  int64(i),
			SlotIdx: int16(i),
		})
	}

	_, stack := tree.FindAndGetStack(PersistentKey(9000), Read)
	leftMostNode := tree.pager.GetNode(stack[len(stack)-1].Node)
	for {
		if leftMostNode == nil {
			break
		}
		leftMostNode.PrintNode()
		old := leftMostNode
		leftMostNode = tree.pager.GetNode(leftMostNode.GetRight())
		tree.pager.Unpin(old, false)
	}

	assert.True(t, true)
}
