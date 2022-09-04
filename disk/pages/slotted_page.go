package pages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"helin/common"
	"helin/disk"
	"unsafe"
)

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
	GetHeader() SlottedPageHeader
	SetHeader(h SlottedPageHeader)

	// vacuum push all content of the page to the rightmost to eliminate fragmentation
	vacuum()

	GetFreeSpace() int

	// DeleteTuple deletes the tuple which is pointed by the value in the slot array at idxAtSlot
	DeleteTuple(idxAtSlot int)
	GetTuple(idxAtSlot int) []byte
	InsertTuple(data []byte) (int, error)
	UpdateTuple(idxAtSlot int, data []byte) error

	// GetNextIdx returns the next not deleted slots idx in the slot array. If it is the last one that is not deleted returns error
	GetNextIdx(currIdxAtSlot int) (int, error)
}

type SlottedPageHeader struct {
	FreeSpacePointer uint32
	SLotArrLen       uint16
	NextPageID       int64
	PrevPageID       int64
}

type SLotArrEntry struct {
	Offset uint32
	Size   uint32
}

const (
	// DELETE_MASK first bit of the TupleSizeType holds tuple's deleted status and, it can be accessed by applying DELETE_MASK
	// to a TupleSizeType instance
	// uint32 comes from SlotArrEntry.Size
	DELETE_MASK uint32 = 1 << (unsafe.Sizeof(uint32(1))*4 - 1)

	LOW_BYTES  = (1 << 32) - 1
	HIGH_BYTES = LOW_BYTES << 32

	SLOT_ARRAY_ENTRY_SIZE = 8
)

var HEADER_SIZE = binary.Size(SlottedPageHeader{})

type SlottedPage struct {
	RawPage
}

func (sp *SlottedPage) GetNextIdx(currIdxAtSlot int) (int, error) {
	arr := sp.getSlotArr()
	if len(arr) <= currIdxAtSlot+1 {
		return 0, errors.New("")
	}

	for i := currIdxAtSlot + 1; i < len(arr); i++ {
		entry := arr[i]
		if !isDeleted(entry) {
			return i, nil
		}
	}

	return 0, errors.New("")
}

func (sp *SlottedPage) vacuum() {
	panic("implement me")
}

func (sp *SlottedPage) GetTuple(idxAtSlot int) []byte {
	entry := sp.getFromSlotArr(idxAtSlot)
	if entry.Size == 0 || isDeleted(entry) {
		return nil
	}

	return sp.GetData()[entry.Offset : entry.Offset+entry.Size]
}

func (sp *SlottedPage) GetFreeSpace() int {
	h := sp.GetHeader()
	startingOffset := HEADER_SIZE + int(h.SLotArrLen)*SLOT_ARRAY_ENTRY_SIZE
	return int(h.FreeSpacePointer) - startingOffset
}

func (sp *SlottedPage) getSlotArr() []SLotArrEntry {
	header := sp.GetHeader()
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

func (sp *SlottedPage) GetHeader() SlottedPageHeader {
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
	h := sp.GetHeader()
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

func (sp *SlottedPage) UpdateTuple(idxAtSlot int, data []byte) error {
	oldData := sp.GetTuple(idxAtSlot)
	if oldData == nil {
		msg := fmt.Sprintf("tried to update a nonexistent or deleted tuple idxAtSlot: %v, pageID: %v", idxAtSlot, sp.GetPageId())
		return errors.New(msg)
	}

	if sp.GetFreeSpace()+len(oldData) < len(data) {
		return errors.New("not enough space in slotted page") // TODO: return typed error. because this error will indicate that the caller should do an delete-insert to do an update
	}

	if err := sp.HardDelete(idxAtSlot); err != nil { // NOTE: this will cause a deadlock when write latches are implemented
		panic(err) // this should not return error
	}

	h := sp.GetHeader()
	h.FreeSpacePointer -= uint32(len(data))
	copy(sp.GetData()[h.FreeSpacePointer:], data)
	sp.SetHeader(h)
	sp.setInSlotArr(idxAtSlot, SLotArrEntry{
		Offset: h.FreeSpacePointer,
		Size:   uint32(len(data)),
	})

	return nil
}

func (sp *SlottedPage) HardDelete(idxAtSlot int) error {
	arr := sp.getSlotArr()
	if idxAtSlot >= len(arr) {
		return errors.New("slot cannot be found")
	}

	entry := arr[idxAtSlot]
	if isDeleted(entry) {
		entry.Size = entry.Size & (^DELETE_MASK) // unset  deleted flag
	}

	h := sp.GetHeader()
	data := sp.GetData()

	copy(data[h.FreeSpacePointer+entry.Size:entry.Offset+entry.Size], data[h.FreeSpacePointer:entry.Offset])
	h.FreeSpacePointer += entry.Size
	deletedEntry := entry
	entry.Size = 0
	entry.Offset = 0
	sp.setInSlotArr(idxAtSlot, entry)
	sp.SetHeader(h)

	// update all tuples' offsets that comes before the deleted one
	for i := 0; i < int(h.SLotArrLen); i++ {
		arr := sp.getSlotArr()
		currEntry := arr[i]
		if currEntry.Offset > deletedEntry.Offset {
			continue
		} else if currEntry.Offset < deletedEntry.Offset && currEntry.Size != 0 { // if size is 0 it means slot is empty hence no need to update it
			currEntry.Offset += deletedEntry.Size
			sp.setInSlotArr(i, currEntry)
		}
	}

	return nil
}

func (sp *SlottedPage) SoftDelete(idxAtSlot int) error {
	arr := sp.getSlotArr()
	if idxAtSlot >= len(arr) {
		return errors.New("slot cannot be found")
	}

	entry := arr[idxAtSlot]
	if isDeleted(entry) {
		return errors.New("slot is already deleted")
	}

	// if raw bytes of tuple size is not 0 and its delete bit is not set
	entry.Size = entry.Size | DELETE_MASK
	sp.setInSlotArr(idxAtSlot, entry)

	return nil
}

func isDeleted(entry SLotArrEntry) bool {
	size := entry.Size
	x := entry.Size & DELETE_MASK
	return x != 0 || size == 0
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

func SlottedPageInstanceFromRawPage(page *RawPage) *SlottedPage {
	return &SlottedPage{
		RawPage: *page,
	}
}

func FormatAsSlottedPage(page *RawPage) *SlottedPage {
	s := &SlottedPage{
		RawPage: *page,
	}

	s.SetHeader(SlottedPageHeader{
		FreeSpacePointer: uint32(disk.PageSize),
		SLotArrLen:       0,
		NextPageID:       0,
		PrevPageID:       0,
	})

	return s
}
