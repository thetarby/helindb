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

type IHeapPage interface {
	GetHeader() HeapPageHeader
	SetHeader(h HeapPageHeader)

	GetFreeSpace() int

	// DeleteTuple deletes the tuple which is pointed by the value in the slot array at idxAtSlot
	DeleteTuple(idxAtSlot int)
	GetTuple(idxAtSlot int) []byte
	InsertTuple(data []byte) (int, error)
	UpdateTuple(idxAtSlot int, data []byte) error

	// GetNextIdx returns the next not deleted slots idx in the slot array. If it is the last one that is not deleted returns error
	GetNextIdx(currIdxAtSlot int) (int, error)
}

type HeapPageHeader struct {
	FreeSpacePointer uint32
	SLotArrLen       uint16
	NextPageID       int64
	PrevPageID       int64
}

type HeapPageArrEntry struct {
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

var HeapPageHeaderSize = binary.Size(HeapPageHeader{})

type HeapPage struct {
	RawPage
}

func (sp *HeapPage) GetNextIdx(currIdxAtSlot int) (int, error) {
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

func (sp *HeapPage) GetTuple(idxAtSlot int) []byte {
	entry := sp.getFromSlotArr(idxAtSlot)
	if entry.Size == 0 || isDeleted(entry) {
		return nil
	}

	return sp.GetData()[entry.Offset : entry.Offset+entry.Size]
}

func (sp *HeapPage) GetFreeSpace() int {
	h := sp.GetHeader()
	startingOffset := HeapPageHeaderSize + int(h.SLotArrLen)*SLOT_ARRAY_ENTRY_SIZE
	return int(h.FreeSpacePointer) - startingOffset
}

func (sp *HeapPage) GetHeader() HeapPageHeader {
	reader := bytes.NewReader(sp.GetData())
	dest := HeapPageHeader{}
	binary.Read(reader, binary.BigEndian, &dest)
	return dest
}

func (sp *HeapPage) SetHeader(h HeapPageHeader) {
	buf := bytes.Buffer{}

	// NOTE: this error is actually the error returned by bytes.Buffer.Write call which always returns nil hence no need to check
	err := binary.Write(&buf, binary.BigEndian, &h)
	common.PanicIfErr(err)

	copy(sp.GetData(), buf.Bytes())
}

func (sp *HeapPage) InsertTuple(data []byte) (int, error) {
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
	sp.setInSlotArr(i, HeapPageArrEntry{
		Offset: h.FreeSpacePointer,
		Size:   uint32(len(data)),
	})
	return i, nil
}

func (sp *HeapPage) UpdateTuple(idxAtSlot int, data []byte) error {
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
	sp.setInSlotArr(idxAtSlot, HeapPageArrEntry{
		Offset: h.FreeSpacePointer,
		Size:   uint32(len(data)),
	})

	return nil
}

func (sp *HeapPage) HardDelete(idxAtSlot int) error {
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

func (sp *HeapPage) SoftDelete(idxAtSlot int) error {
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

func (sp *HeapPage) getSlotArr() []HeapPageArrEntry {
	header := sp.GetHeader()
	return readEntry(int(header.SLotArrLen), sp.GetData()[HeapPageHeaderSize:])
}

func (sp *HeapPage) getFromSlotArr(idx int) HeapPageArrEntry {
	// TODO: more performant impl.
	arr := sp.getSlotArr()
	return arr[idx]
}

func (sp *HeapPage) setInSlotArr(idx int, val HeapPageArrEntry) {
	offset := int(HeapPageHeaderSize) + SLOT_ARRAY_ENTRY_SIZE*idx
	buf := bytes.Buffer{}

	// NOTE: this error is actually the error returned by bytes.Buffer.Write call which always returns nil hence no need to check
	err := binary.Write(&buf, binary.BigEndian, &val)
	common.PanicIfErr(err)

	if offset >= disk.PageSize {
		panic("page overflow error")
	}

	copy(sp.GetData()[offset:], buf.Bytes())
}

func isDeleted(entry HeapPageArrEntry) bool {
	size := entry.Size
	x := entry.Size & DELETE_MASK
	return x != 0 || size == 0
}

func readEntry(count int, data []byte) []HeapPageArrEntry {
	reader := bytes.NewReader(data)
	res := make([]HeapPageArrEntry, 0)
	for i := 0; i < int(count); i++ {
		x := HeapPageArrEntry{}
		err := binary.Read(reader, binary.BigEndian, &x) // TODO: look at possible errors
		common.PanicIfErr(err)
		res = append(res, x)
	}
	return res
}

func AsHeapPage(page *RawPage) *HeapPage {
	return &HeapPage{
		RawPage: *page,
	}
}

func InitHeapPage(page *RawPage) *HeapPage {
	s := &HeapPage{
		RawPage: *page,
	}

	s.SetHeader(HeapPageHeader{
		FreeSpacePointer: uint32(disk.PageSize),
		SLotArrLen:       0,
		NextPageID:       0,
		PrevPageID:       0,
	})

	return s
}
