package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"helin/disk/pages"
	"sort"
)

var NotEnoughSpace error = errors.New("not enough space")

type SlottedPage struct {
	PersistentPage
}

type SlottedPageHeader struct {
	FreeSpacePointer uint16
	SlotArrSize      uint16
	EmptyBytes       uint16
}

type SLotArrEntry struct {
	Offset uint16
}

var HEADER_SIZE = binary.Size(SlottedPageHeader{})
var SLOT_ARR_ENTRY_SIZE = binary.Size(SLotArrEntry{})

func (sp *SlottedPage) GetHeader() SlottedPageHeader {
	reader := bytes.NewReader(sp.GetData())
	dest := SlottedPageHeader{}
	binary.Read(reader, binary.BigEndian, &dest)
	return dest
}

func (sp *SlottedPage) SetHeader(h SlottedPageHeader) {
	buf := bytes.Buffer{}

	// NOTE: this error is actually the error returned by bytes.Buffer.Write call which always returns nil hence no need to check
	if err := binary.Write(&buf, binary.BigEndian, &h); err != nil {
		panic(err)
	}

	copy(sp.GetData(), buf.Bytes())
}

func (sp *SlottedPage) GetFreeSpace() int {
	h := sp.GetHeader()
	startingOffset := HEADER_SIZE + int(h.SlotArrSize)
	return int(h.FreeSpacePointer) - startingOffset
}

func (sp *SlottedPage) FillFactor() float32 {
	h := sp.GetHeader()
	payloadSize := len(sp.GetData()) - int(h.FreeSpacePointer)
	if payloadSize == 0 {
		return 1
	}
	filled := payloadSize - int(h.EmptyBytes)
	return float32(filled) / float32(payloadSize)
}

func (sp *SlottedPage) GetAt(idx int) []byte {
	slots := sp.getSlotArr()
	entry := slots[idx]

	if entry.Offset == 0 || isDeleted(entry) {
		return nil
	}

	d := sp.GetData()
	valSize, n := binary.Uvarint(d[entry.Offset:])

	return d[entry.Offset+uint16(n) : entry.Offset+uint16(n)+uint16(valSize)]
}

func (sp *SlottedPage) InsertAt(idx int, data []byte) error {
	h := sp.GetHeader()

	temp := make([]byte, 4)
	n := binary.PutUvarint(temp, uint64(len(data)))
	h.FreeSpacePointer -= uint16(len(data) + n)
	if h.FreeSpacePointer <= (h.SlotArrSize+1) * 2 { // 2 is one entry size in slotarr (uint16) 
		return NotEnoughSpace
	} 

	copy(sp.GetData()[h.FreeSpacePointer:], temp[:n])
	copy(sp.GetData()[h.FreeSpacePointer+uint16(n):], data)
	arr := sp.getSlotArr()

	if len(arr) <= idx {
		for len(arr) <= idx {
			if len(arr) == idx {
				arr = append(arr, SLotArrEntry{Offset: h.FreeSpacePointer})
			} else {
				arr = append(arr, SLotArrEntry{})
			}
		}
	} else {
		arr = append(arr[:idx+1], arr[idx:]...)
		arr[idx] = SLotArrEntry{
			Offset: h.FreeSpacePointer,
		}
	}

	h.SlotArrSize = uint16(len(arr))
	sp.setSlotArr(arr)
	sp.SetHeader(h)

	return nil
}

func (sp *SlottedPage) SetAt(idx int, data []byte) error {
	// TODO: optimize this. try directly to put same place as old data
	arr := sp.getSlotArr()
	if len(arr) <= idx{
		return sp.InsertAt(idx, data) // TODO: bad impl.
	}
	offset := arr[idx].Offset
	d := sp.GetData()
	valSize, _ := binary.Uvarint(d[offset:])
	
	// no need to account for uvarint valsize because if new data is smaller its uvarint valsize must be of equal
	// length or shorter
	if int(valSize) >= len(data){
		newValSize := len(data)
		n := binary.PutUvarint(d[offset:], uint64(newValSize))
		copy(d[int(offset)+n:], data)
		return nil
	}

	if err := sp.InsertAt(idx, data); err != nil {
		return err
	}

	return sp.DeleteAt(idx+1)
}

func (sp *SlottedPage) DeleteAt(idx int) error {
	arr := sp.getSlotArr()
	if idx >= len(arr) {
		return errors.New("slot cannot be found")
	}

	e := arr[idx]
	valSize, n := binary.Uvarint(sp.GetData()[e.Offset:])
	arr = append(arr[:idx], arr[idx+1:]...)
	
	h := sp.GetHeader()
	h.SlotArrSize--
	h.EmptyBytes += uint16(valSize) + uint16(n)
	
	sp.setSlotArr(arr)
	sp.SetHeader(h)

	return nil
}

func (sp *SlottedPage) Vacuum() (int, error) {
	// sort slice arr by offset descending
	arr := sp.getSlotArr()
	idxArr := make([]int, len(arr))
	for i := 0; i < len(arr); i++ {
		idxArr[i] = i
	}

	sort.Slice(idxArr, func(i, j int) bool {
		return arr[idxArr[i]].Offset > arr[idxArr[j]].Offset
	})

	d := sp.GetData()
	newFreeSpace := len(d)
	for _, v := range idxArr {
		e := arr[v]
		valSize, n := binary.Uvarint(d[e.Offset:])
		size := int(valSize) + n

		shiftSize := newFreeSpace - (int(e.Offset) + int(size))
		newFreeSpace -= int(size)

		copy(d[int(e.Offset)+shiftSize:], d[e.Offset:e.Offset+uint16(size)])
		arr[v].Offset = uint16(newFreeSpace)
	}

	h := sp.GetHeader()
	h.FreeSpacePointer = uint16(newFreeSpace)
	h.EmptyBytes = 0
	sp.SetHeader(h)
	sp.setSlotArr(arr)
	return 0, nil
}

func (sp *SlottedPage) getSlotArr() []SLotArrEntry {
	h := sp.GetHeader()
	buf := sp.GetData()[HEADER_SIZE:]
	arr := make([]SLotArrEntry, 0)
	for n := 0; n < int(h.SlotArrSize); n++ {
		e := SLotArrEntry{}
		offset := binary.BigEndian.Uint16(buf[n*2:]) // 2 is uint16 size 
		e.Offset = offset
		arr = append(arr, e)
	}

	return arr
}

func (sp *SlottedPage) setSlotArr(arr []SLotArrEntry) {
	buf := sp.GetData()[HEADER_SIZE:]
	n := 0
	for _, e := range arr {
		binary.BigEndian.PutUint16(buf[n:], e.Offset)
		n += 2 // size of uint16
	}
	if n + HEADER_SIZE > int(sp.GetHeader().FreeSpacePointer){
		panic("") // TODO: check these
	}
}

func (sp *SlottedPage) values() []byte {
	return sp.GetData()[sp.GetHeader().FreeSpacePointer:]
}

func InitSlottedPage(rawPage pages.RawPage) SlottedPage{
	sp :=  SlottedPage{
		PersistentPage: PersistentPage{
			RawPage: rawPage,
		},
	}

	sp.SetHeader(SlottedPageHeader{
		FreeSpacePointer: uint16(len(sp.Data)),
		SlotArrSize:      0,
	})

	return sp
}

func CastSlottedPage(rawPage pages.RawPage) SlottedPage{
	return SlottedPage{
		PersistentPage: PersistentPage{
			RawPage: rawPage,
		},
	}
}

func isDeleted(entry SLotArrEntry) bool {
	return false
}
