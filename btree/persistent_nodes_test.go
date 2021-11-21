package btree

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
	"testing"
	"time"
)

func TestPersistentLeafNode_SetKeyAt(t *testing.T) {
	node := PersistentLeafNode{PersistentPage: NewNoopPersistentPage(1), serializer: &PersistentKeySerializer{}}
	node.setKeyAt(0, PersistentKey(1))
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	node.setValueAt(0, p)

	key := node.GetKeyAt(0)
	val := node.GetValueAt(0).(SlotPointer)

	assert.Equal(t, PersistentKey(1), key)
	assert.Equal(t, p, val)
}

func TestPersistentLeafNode_SetKeyAt_String_Key(t *testing.T) {
	node := PersistentLeafNode{PersistentPage: NewNoopPersistentPage(1), serializer: &StringKeySerializer{Len: 5}}
	node.setKeyAt(0, StringKey("selam"))
	p := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	node.setValueAt(0, p)

	key := node.GetKeyAt(0)
	val := node.GetValueAt(0).(SlotPointer)

	assert.Equal(t, StringKey("selam"), key)
	assert.Equal(t, p, val)
}

func TestPersistentLeafNode_Should_Insert_To_Correct_Locations(t *testing.T) {
	node := PersistentLeafNode{PersistentPage: NewNoopPersistentPage(1), serializer: &PersistentKeySerializer{}}
	slotP := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}
	for i := 0; i < 10; i++ {
		idx, found := node.findKey(PersistentKey(i))
		assert.False(t, found)
		node.InsertAt(idx, PersistentKey(i), slotP)
	}

	for i := 0; i < 10; i++ {
		key := node.GetKeyAt(i)
		val := node.GetValueAt(i)

		assert.Equal(t, PersistentKey(i), key)
		assert.Equal(t, slotP, val.(SlotPointer))
	}
}

func TestPersistentLeafNode_FindKey_Should_Find_Correct_Locations_When_Keys_Are_Inserted_Randomly(t *testing.T) {
	node := PersistentLeafNode{PersistentPage: NewNoopPersistentPage(1), serializer: &PersistentKeySerializer{}}
	slotP := SlotPointer{
		PageId:  10,
		SlotIdx: 10,
	}

	a := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	for _, item := range a {
		idx, found := node.findKey(PersistentKey(item))
		assert.False(t, found)
		node.InsertAt(idx, PersistentKey(item), slotP)
	}

	for i := 0; i < 10; i++ {
		key := node.GetKeyAt(i)
		val := node.GetValueAt(i)

		assert.Equal(t, PersistentKey(i+1), key)
		assert.Equal(t, slotP, val.(SlotPointer))
	}
}

func TestPersistentInternalNode_SetKeyAt_0_Should_Handle_First_Pointer(t *testing.T) {
	node := PersistentInternalNode{PersistentPage: NewNoopPersistentPage(1), serializer: &PersistentKeySerializer{}}
	node.setKeyAt(0, PersistentKey(15))
	p := Pointer(54)
	node.setValueAt(0, p)

	key := node.GetKeyAt(0)
	val := node.GetValueAt(0).(Pointer)

	assert.Equal(t, PersistentKey(15), key)
	assert.Equal(t, p, val)
}

func TestPersistentInternalNode_Should_Insert_To_Correct_Locations_2(t *testing.T) {
	node := PersistentInternalNode{PersistentPage: NewNoopPersistentPage(1), serializer: &PersistentKeySerializer{}}
	node.setKeyAt(0, PersistentKey(15))
	p := Pointer(54)
	node.setValueAt(0, p)

	node.setKeyAt(1, PersistentKey(55))
	p2 := Pointer(55)
	node.setValueAt(1, p2)

	key := node.GetKeyAt(0)
	val := node.GetValueAt(0).(Pointer)

	key2 := node.GetKeyAt(1)
	val2 := node.GetValueAt(1).(Pointer)

	assert.Equal(t, PersistentKey(15), key)
	assert.Equal(t, p, val)
	assert.Equal(t, PersistentKey(55), key2)
	assert.Equal(t, p2, val2)
}

func TestPersistentInternalNode_Should_Insert_To_Correct_Locations(t *testing.T) {
	node := PersistentInternalNode{PersistentPage: NewNoopPersistentPage(1), serializer: &PersistentKeySerializer{}}

	for i := 1; i < 10; i++ {
		idx, found := node.findKey(PersistentKey(i))
		assert.False(t, found)
		node.InsertAt(idx, PersistentKey(i), Pointer(i))
	}

	for i := 0; i < 9; i++ {
		key := node.GetKeyAt(i)
		val := node.GetValueAt(i + 1)

		assert.Equal(t, PersistentKey(i+1), key)
		assert.Equal(t, Pointer(i+1), val.(Pointer))
	}
}

func TestPersistentInternalNode_FindKey_Should_Find_Correct_Locations_When_Keys_Are_Inserted_Randomly(t *testing.T) {
	node := NewPersistentInternalNode(1)
	node.serializer = &PersistentKeySerializer{}
	a := []int{2, 3, 4, 5, 6, 7, 8, 9, 10} // 1 is inserted when creating
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	for _, item := range a {
		idx, found := node.findKey(PersistentKey(item))
		assert.False(t, found)
		node.InsertAt(idx, PersistentKey(item), Pointer(item))
	}

	// check first pointer
	val := node.GetValueAt(0)
	assert.Equal(t, Pointer(1), val.(Pointer))

	for i := 0; i < 9; i++ {
		key := node.GetKeyAt(i)
		val := node.GetValueAt(i + 1)

		assert.Equal(t, PersistentKey(i+2), key)
		assert.Equal(t, Pointer(i+2), val.(Pointer))
	}
}
