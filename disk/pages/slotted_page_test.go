package pages

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/stretchr/testify/assert"
	"helin/disk"
	"testing"
)

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
