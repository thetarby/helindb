package pages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"helin/common"
	"helin/disk"
	"unsafe"
)

type TupleSizeType uint32

/**
 * Slotted page format:
 *  ---------------------------------------------------------
 *  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
 *  ---------------------------------------------------------
 *                                ^
 *                                free space pointer
 *
 *  Header format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  ----------------------------------------------------------------------------
 *  ----------------------------------------------------------------
 *  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
 *  ----------------------------------------------------------------
 *
 */

type ISlottedPage interface {
	getSlotArr() []SLotArrEntry
	getFromSlotArr(idx int) SLotArrEntry
	setInSlotArr(idx int, val SLotArrEntry)
	appendSlotArr(val SLotArrEntry)
	getHeader() SlottedPageHeader
	SetHeader(h SlottedPageHeader)

	// vacuum push all content of the page to the rightmost to eliminate fragmentation
	vacuum()

	InsertTuple(data []byte) (int, error)
	GetFreeSpace() int

	// DeleteTuple deletes the tuple which is pointed by the value in the slot array at idxAtSlot
	DeleteTuple(idxAtSlot int)

	GetTuple(idxAtSlot int) []byte
}

type SlottedPageHeader struct {
	FreeSpacePointer uint32
	SLotArrLen       uint16
}

type SLotArrEntry struct {
	Offset uint32
	Size   uint32
}

const (
	// DELETE_MASK first bit of the TupleSizeType holds tuple's deleted status and, it can be accessed by applying DELETE_MASK
	// to a TupleSizeType instance
	DELETE_MASK = 1<<unsafe.Sizeof(TupleSizeType(1))*8 - 1

	LOW_BYTES  = (1 << 32) - 1
	HIGH_BYTES = LOW_BYTES << 32

	SLOT_ARRAY_ENTRY_SIZE = 8
)

var HEADER_SIZE = binary.Size(SlottedPageHeader{})

type SlottedPage struct {
	RawPage
}

func (sp *SlottedPage) vacuum() {
	panic("implement me")
}

func (sp *SlottedPage) GetTuple(idxAtSlot int) []byte {
	entry := sp.getFromSlotArr(idxAtSlot)
	if entry.Size == 0 {
		return nil
	}

	return sp.GetData()[entry.Offset : entry.Offset+entry.Size]
}

func (sp *SlottedPage) GetFreeSpace() int {
	h := sp.getHeader()
	startingOffset := HEADER_SIZE + int(h.SLotArrLen)*SLOT_ARRAY_ENTRY_SIZE
	return int(h.FreeSpacePointer) - startingOffset
}

func (sp *SlottedPage) getSlotArr() []SLotArrEntry {
	header := sp.getHeader()
	return readSLotArrEntrySliceFromBytes(int(header.SLotArrLen), sp.GetData()[HEADER_SIZE:])
}

func (sp *SlottedPage) getFromSlotArr(idx int) SLotArrEntry {
	// TODO: more performant impl.
	arr := sp.getSlotArr()
	return arr[idx]
}

func (sp *SlottedPage) setInSlotArr(idx int, val SLotArrEntry) {
	offset := int(HEADER_SIZE) + SLOT_ARRAY_ENTRY_SIZE*idx
	buf := bytes.Buffer{}

	// NOTE: this error is actually the error returned by bytes.Buffer.Write call which always returns nil hence no need to check
	err := binary.Write(&buf, binary.BigEndian, &val)
	common.PanicIfErr(err)

	if offset >= disk.PageSize {
		panic("page overflow error")
	}

	copy(sp.GetData()[offset:], buf.Bytes())
}

func (sp *SlottedPage) appendSlotArr(val SLotArrEntry) {
	h := sp.getHeader()
	h.SLotArrLen++
	defer sp.SetHeader(h)

	sp.setInSlotArr(int(h.SLotArrLen)-1, val)
}

func (sp *SlottedPage) getHeader() SlottedPageHeader {
	reader := bytes.NewReader(sp.GetData())
	dest := SlottedPageHeader{}
	binary.Read(reader, binary.BigEndian, &dest)
	return dest
}

func (sp *SlottedPage) SetHeader(h SlottedPageHeader) {
	buf := bytes.Buffer{}

	// NOTE: this error is actually the error returned by bytes.Buffer.Write call which always returns nil hence no need to check
	err := binary.Write(&buf, binary.BigEndian, &h)
	common.PanicIfErr(err)

	copy(sp.GetData(), buf.Bytes())
}

func (sp *SlottedPage) InsertTuple(data []byte) (int, error) {
	/*
		first check if there is enough space in the page, if not return error
		second iterate slot arr to see if there is an empty slot, meaning a slot with size 0
	*/
	//sp.WLatch()
	//defer sp.WUnlatch()
	if sp.GetFreeSpace() < len(data)+SLOT_ARRAY_ENTRY_SIZE {
		return 0, errors.New("not enough space in slotted page")
	}

	arr := sp.getSlotArr()
	i := 0
	for ; i < len(arr); i++ {
		if arr[i].Size == 0 {
			break
		}
	}

	// if an empty slot is found, copy data and set free space pointer to the starting point of new data
	h := sp.getHeader()
	h.FreeSpacePointer -= uint32(len(data))
	if i == len(arr) {
		h.SLotArrLen++
	}
	copy(sp.GetData()[h.FreeSpacePointer:], data)
	sp.SetHeader(h)
	sp.setInSlotArr(i, SLotArrEntry{
		Offset: h.FreeSpacePointer,
		Size:   uint32(len(data)),
	})
	return i, nil
}

func (sp *SlottedPage) DeleteTuple(idxAtSlot int) {
	sp.setInSlotArr(idxAtSlot, SLotArrEntry{
		Offset: 0,
		Size:   0,
	})
	sp.vacuum()
}

func readSLotArrEntrySliceFromBytes(count int, data []byte) []SLotArrEntry {
	reader := bytes.NewReader(data)
	res := make([]SLotArrEntry, 0)
	for i := 0; i < int(count); i++ {
		x := SLotArrEntry{}
		err := binary.Read(reader, binary.BigEndian, &x) // TODO: look at possible errors
		common.PanicIfErr(err)
		res = append(res, x)
	}
	return res
}
