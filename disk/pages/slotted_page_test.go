package pages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/common"
	"helin/disk"
	"testing"
)

type SlotIdxOffsetPair struct {
	offset int
	idx    int
}

func newSlottedPageTestInstance() SlottedPage {
	p := SlottedPage{RawPage: *NewRawPage(1)}
	p.SetHeader(SlottedPageHeader{
		FreeSpacePointer: uint32(disk.PageSize),
		SLotArrLen:       0,
	})
	return p
}

func TestInsert_Tuple(t *testing.T) {
	p := newSlottedPageTestInstance()
	toInsert := []byte("selam")
	p.InsertTuple(toInsert)
	res := p.GetTuple(0)

	assert.Len(t, p.getSlotArr(), 1)
	assert.Equal(t, toInsert, res)
}

func TestAll_Inserted_Should_Be_Found(t *testing.T) {
	p := newSlottedPageTestInstance()
	n := 100
	for i := 0; i < n; i++ {
		toInsert := []byte(fmt.Sprintf("selam_%v", i))
		p.InsertTuple(toInsert)
	}
	assert.Len(t, p.getSlotArr(), n)

	for i := 0; i < n; i++ {
		res := p.GetTuple(i)
		assert.Equal(t, []byte(fmt.Sprintf("selam_%v", i)), res)
	}
}

func TestInsert_Tuple_Should_Return_Error_When_There_Is_No_Enough_Space_Left(t *testing.T) {
	p := newSlottedPageTestInstance()
	freeSpace := p.GetFreeSpace()

	toInsert := make([]byte, freeSpace/3)

	_, err := p.InsertTuple(toInsert)
	assert.NoError(t, err)
	_, err = p.InsertTuple(toInsert)
	assert.NoError(t, err)

	_, err = p.InsertTuple(toInsert)
	assert.Error(t, err)
}

func TestSlottedPage_HardDelete_Should_Update_Other_Slots_Offsets_Correctly_When_Deleted_Item_From_End_Of_Tuple_Space(t *testing.T) {
	p := newSlottedPageTestInstance()
	tupleSize := 10
	insertCount := 50

	pairs := make([]SlotIdxOffsetPair, 0, insertCount)

	for i := 0; i < insertCount; i++ {
		toInsert := make([]byte, tupleSize)
		idx, err := p.InsertTuple(toInsert)
		assert.NoError(t, err)
		pairs = append(pairs, SlotIdxOffsetPair{idx: idx, offset: int(p.getFromSlotArr(idx).Offset)})
	}

	p.HardDelete(pairs[0].idx)

	for i, pair := range pairs {
		if i == 0 {
			entry := p.getFromSlotArr(i)
			assert.True(t, isDeleted(entry))
			break // this is deleted
		}
		newOffset := p.getFromSlotArr(pair.idx).Offset
		assert.Equal(t, pair.offset-tupleSize, int(newOffset))
	}

}

func TestSlottedPage_HardDelete_Should_Update_Other_Slots_Offsets_Correctly_When_Deleted_Item_From_Middle_Of_Tuple_Space(t *testing.T) {
	p := newSlottedPageTestInstance()
	tupleSize := 10
	insertCount := 50
	deleteIdx := 25

	pairs := make([]SlotIdxOffsetPair, 0, insertCount)

	for i := 0; i < insertCount; i++ {
		toInsert := make([]byte, tupleSize)
		idx, err := p.InsertTuple(toInsert)
		assert.NoError(t, err)
		pairs = append(pairs, SlotIdxOffsetPair{idx: idx, offset: int(p.getFromSlotArr(idx).Offset)})
	}

	p.HardDelete(pairs[deleteIdx].idx)

	for i, pair := range pairs {
		if i == deleteIdx {
			entry := p.getFromSlotArr(i)
			assert.True(t, isDeleted(entry))
			break // this is deleted
		} else if i < deleteIdx {
			newOffset := p.getFromSlotArr(pair.idx).Offset
			assert.Equal(t, pair.offset, int(newOffset))
		} else {
			newOffset := p.getFromSlotArr(pair.idx).Offset
			assert.Equal(t, pair.offset-tupleSize, int(newOffset))
		}
	}

}

func TestSlottedPage_HardDeleted_Slots_Should_Not_Be_Found(t *testing.T) {
	p := newSlottedPageTestInstance()
	tupleSize := 10
	insertCount := 50
	toDeleteIndexes := []int{1, 2, 3, 16}

	pairs := make([]SlotIdxOffsetPair, 0, insertCount)

	for i := 0; i < insertCount; i++ {
		toInsert := make([]byte, tupleSize)
		toInsert[0] = byte(i)
		idx, err := p.InsertTuple(toInsert)
		assert.NoError(t, err)
		pairs = append(pairs, SlotIdxOffsetPair{idx: idx, offset: int(p.getFromSlotArr(idx).Offset)})
	}

	for _, idx := range toDeleteIndexes {
		p.HardDelete(pairs[idx].idx)
	}

	for i, pair := range pairs {
		println(i)
		if common.Contains(toDeleteIndexes, i) {
			entry := p.getFromSlotArr(pair.idx)
			require.True(t, isDeleted(entry))
			continue
		}

		tuple := p.GetTuple(pair.idx)
		require.Equal(t, byte(i), tuple[0])
	}
}

func TestSlottedPage_HardDeleted_Slots_Should_Not_Be_Found_2(t *testing.T) {
	p := newSlottedPageTestInstance()
	tupleSize := 10
	insertCount := 50
	toDeleteIndexes := []int{3, 12, 23, 34}

	pairs := make([]SlotIdxOffsetPair, 0, insertCount)

	for i := 0; i < insertCount; i++ {
		toInsert := make([]byte, tupleSize)
		toInsert[0] = byte(i)
		idx, err := p.InsertTuple(toInsert)
		assert.NoError(t, err)
		pairs = append(pairs, SlotIdxOffsetPair{idx: idx, offset: int(p.getFromSlotArr(idx).Offset)})
	}

	for _, idx := range toDeleteIndexes {
		p.HardDelete(idx)
	}

	for i, pair := range pairs {
		if common.Contains(toDeleteIndexes, pair.idx) {
			entry := p.getFromSlotArr(i)
			assert.True(t, isDeleted(entry))
			continue
		}

		tuple := p.GetTuple(pair.idx)
		assert.Equal(t, byte(i), tuple[0])
	}
}

func TestSlottedPage_Update_Should_Not_Return_Error_When_There_Is_Enough_Space_In_Page(t *testing.T) {
	p := newSlottedPageTestInstance()
	tupleSize := p.GetFreeSpace() - SLOT_ARRAY_ENTRY_SIZE
	data := make([]byte, tupleSize)
	idx, err := p.InsertTuple(data)
	require.NoError(t, err)

	data[0] = byte('x')
	err = p.UpdateTuple(idx, data)
	require.NoError(t, err)

	newData := p.GetTuple(idx)
	require.Equal(t, byte('x'), newData[0])
}

func TestReadSLotArrEntrySliceFromBytes(t *testing.T) {
	test := []SLotArrEntry{{
		Offset: 1,
		Size:   1,
	}, {
		Offset: 2,
		Size:   2,
	}}

	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, &test)
	assert.NoError(t, err)

	res := readSLotArrEntrySliceFromBytes(len(test), buf.Bytes())

	assert.ElementsMatch(t, test, res)
}
