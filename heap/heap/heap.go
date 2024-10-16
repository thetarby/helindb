package heap

import (
	"encoding/binary"
	"fmt"
	"helin/common"
	"helin/transaction"
)

/*
	NOTES: heap is not thread-safe. It shouldn't be called in parallel.
*/

// TODO: don't serialize whole header when a field is changed
// TODO: put constraints on header max size, max value size and then optimize header serialization for no alloc

type SlotEntry struct {
	Empty  bool
	Offset uint16
	PageID uint64
	Len    uint16
}

var slotEntrySize = binary.Size(SlotEntry{})

type Header struct {
	SlotArr         []SlotEntry
	PageIDArr       []uint64
	FreeSpaceOffset uint16
}

func (header *Header) asBytes() []byte {
	res := make([]byte, 0)

	res = binary.BigEndian.AppendUint16(res, uint16(len(header.SlotArr)))

	for _, entry := range header.SlotArr {
		res = binary.BigEndian.AppendUint16(res, entry.Offset)
		res = binary.BigEndian.AppendUint16(res, entry.Len)
		res = binary.BigEndian.AppendUint64(res, entry.PageID)
		res = append(res, byte(1))
	}

	res = binary.BigEndian.AppendUint16(res, uint16(len(header.PageIDArr)))

	for _, pageID := range header.PageIDArr {
		res = binary.BigEndian.AppendUint64(res, pageID)
	}

	res = binary.BigEndian.AppendUint16(res, header.FreeSpaceOffset)

	return res
}

func writeHeader(header *Header, dst []byte) int {
	n := 0

	binary.BigEndian.PutUint16(dst[n:], uint16(len(header.SlotArr)))
	n += 2

	for _, entry := range header.SlotArr {
		binary.BigEndian.PutUint16(dst[n:], entry.Offset)
		binary.BigEndian.PutUint16(dst[n+2:], entry.Len)
		binary.BigEndian.PutUint64(dst[n+4:], entry.PageID)
		dst[n+12] = byte(1)

		n += 13
	}

	binary.BigEndian.PutUint16(dst[n:], uint16(len(header.PageIDArr)))
	n += 2

	for _, pageID := range header.PageIDArr {
		binary.BigEndian.PutUint64(dst[n:], pageID)
		n += 8
	}

	binary.BigEndian.PutUint16(dst[n:], header.FreeSpaceOffset)

	return n + 2
}

func readHeader(src []byte) *Header {
	n := 0

	slotArrLen := binary.BigEndian.Uint16(src)
	n += 2

	slotArr := make([]SlotEntry, 0)
	for i := 0; i < int(slotArrLen); i++ {
		entry := SlotEntry{}
		entry.Offset = binary.BigEndian.Uint16(src[n:])
		entry.Len = binary.BigEndian.Uint16(src[n+2:])
		entry.PageID = binary.BigEndian.Uint64(src[n+4:])
		entry.Empty = src[n+12] == 1

		n += slotEntrySize

		slotArr = append(slotArr, entry)
	}

	pageArrLen := binary.BigEndian.Uint16(src[n:])
	n += 2

	pageArr := make([]uint64, 0)
	for i := 0; i < int(pageArrLen); i++ {
		pageID := binary.BigEndian.Uint64(src[n:])
		n += 8
		pageArr = append(pageArr, pageID)
	}

	header := Header{}
	header.FreeSpaceOffset = binary.BigEndian.Uint16(src[n:])
	header.PageIDArr = pageArr
	header.SlotArr = slotArr
	return &header
}

type Heap struct {
	HeadPageID uint64
	PageSize   uint16
	Pager      Pager
	maxSize    int
}

func InitHeap(txn transaction.Transaction, slotSize, maxPageSizeForRecord int, pageSize uint16, pager Pager) (*Heap, error) {
	headPage, err := pager.CreatePage(txn)
	if err != nil {
		return nil, err
	}

	slotArr := make([]SlotEntry, slotSize)
	pageIDArr := make([]uint64, 0)

	header := Header{
		SlotArr:         slotArr,
		PageIDArr:       pageIDArr,
		FreeSpaceOffset: 0,
	}

	b := header.asBytes()
	if err := headPage.CopyAt(txn, 0, b); err != nil {
		return nil, err
	}

	maxHeapPageSize := (maxPageSizeForRecord+1)*slotSize + (maxPageSizeForRecord + 1) + 1
	header.FreeSpaceOffset = uint16(len(b) + (8 * maxHeapPageSize))
	if err := headPage.CopyAt(txn, 0, header.asBytes()); err != nil {
		return nil, err
	}
	headPage.Release(true)

	return &Heap{
		HeadPageID: headPage.GetPageID(),
		PageSize:   pageSize,
		Pager:      pager,
		maxSize:    maxPageSizeForRecord * int(pageSize),
	}, nil
}

func OpenHeap(headPageID uint64, maxPageSizeForRecord int, pageSize uint16, pager Pager) (*Heap, error) {
	return &Heap{
		HeadPageID: headPageID,
		PageSize:   pageSize,
		Pager:      pager,
		maxSize:    maxPageSizeForRecord * int(pageSize),
	}, nil
}

func (h *Heap) Insert(txn transaction.Transaction, data []byte) (int, error) {
	header, err := h.getHeader(txn)
	if err != nil {
		return 0, err
	}

	for i, entry := range header.SlotArr {
		if entry.Len == 0 {
			return i, h.SetAt(txn, i, data)
		}
	}

	return 0, fmt.Errorf("exceeded slot capacity: %v", len(header.SlotArr))
}

func (h *Heap) SetAt(txn transaction.Transaction, idx int, data []byte) error {
	header, err := h.getHeader(txn)
	if err != nil {
		return err
	}

	var firstPageID uint64
	if len(header.PageIDArr) > 0 {
		firstPageID = header.PageIDArr[len(header.PageIDArr)-1]
	} else {
		firstPageID = h.HeadPageID
	}

	fp, err := h.Pager.GetPageToWrite(txn, firstPageID)
	if err != nil {
		return err
	}

	dataOffset := 0
	pageOffset := header.FreeSpaceOffset
	page := fp
	for {
		numberOfBytesToWrite := min(int(h.PageSize)-int(pageOffset), len(data)-dataOffset)
		if err := page.CopyAt(txn, pageOffset, data[dataOffset:dataOffset+numberOfBytesToWrite]); err != nil {
			return err
		}

		dataOffset += numberOfBytesToWrite
		pageOffset += uint16(numberOfBytesToWrite)
		page.Release(true)

		if dataOffset == len(data) {
			break
		}

		// if page is full create new page
		if int(pageOffset) == int(h.PageSize) {
			page, err = h.Pager.CreatePage(txn)
			if err != nil {
				return err
			}

			pageOffset = 0
			header.PageIDArr = append(header.PageIDArr, page.GetPageID())
		}
	}

	header.SlotArr[idx].Offset = header.FreeSpaceOffset
	header.SlotArr[idx].Empty = false
	header.SlotArr[idx].PageID = fp.GetPageID()
	header.SlotArr[idx].Len = uint16(len(data))
	header.FreeSpaceOffset = pageOffset

	if err := h.freeEmptyPages(txn, header); err != nil {
		return err
	}

	if err := h.setHeader(txn, header); err != nil {
		return err
	}

	return nil
}

func (h *Heap) GetAt(txn transaction.Transaction, idx int) ([]byte, error) {
	header, err := h.getHeader(txn)
	if err != nil {
		return nil, err
	}

	entry := header.SlotArr[idx]

	firstPageID := entry.PageID
	fp, err := h.Pager.GetPageToRead(txn, firstPageID)
	if err != nil {
		return nil, err
	}

	res := make([]byte, 0)
	totalRead := uint16(0)
	page := fp
	pageOffset := entry.Offset
	for {
		numberOfBytesToRead := min(entry.Len-totalRead, h.PageSize-(pageOffset))

		d := page.GetData()
		res = append(res, d[pageOffset:pageOffset+numberOfBytesToRead]...)

		totalRead += numberOfBytesToRead
		pageOffset = 0
		page.Release(false)
		if totalRead == entry.Len {
			break
		} else {
			nextPageID := h.nextPage(header, page.GetPageID())
			common.Assert(nextPageID != 0, "corrupt heap")

			page, err = h.Pager.GetPageToRead(txn, nextPageID)
			if err != nil {
				return nil, err
			}
		}
	}

	return res, nil
}

func (h *Heap) DeleteAt(txn transaction.Transaction, idx int) error {
	header, err := h.getHeader(txn)
	if err != nil {
		return err
	}

	header.SlotArr[idx].Len = 0
	return h.setHeader(txn, header)
}

func (h *Heap) Count(txn transaction.Transaction) (int, error) {
	header, err := h.getHeader(txn)
	if err != nil {
		return 0, err
	}

	count := 0
	for _, entry := range header.SlotArr {
		if entry.Len > 0 {
			count++
		}
	}

	return count, nil
}

func (h *Heap) GetPageId() uint64 {
	return h.HeadPageID
}

func (h *Heap) Free(txn transaction.Transaction) error {
	header, err := h.getHeader(txn)
	if err != nil {
		return err
	}

	for _, pid := range header.PageIDArr {
		if err := h.Pager.FreePage(txn, pid); err != nil {
			return err
		}
	}

	return h.Pager.FreePage(txn, h.HeadPageID)
}

func (h *Heap) FreeEmptyPages(txn transaction.Transaction) error {
	header, err := h.getHeader(txn)
	if err != nil {
		return err
	}

	if err := h.freeEmptyPages(txn, header); err != nil {
		return err
	}

	return h.setHeader(txn, header)
}

func (h *Heap) freeEmptyPages(txn transaction.Transaction, header *Header) error {
	usedPageIdx := make(map[int]bool)
	for i := range header.PageIDArr {
		usedPageIdx[i] = false
	}

	for _, entry := range header.SlotArr {
		if entry.Len == 0 {
			continue
		}

		pageIDIdx := h.pageIDIdx(header, entry.PageID)

		totalRead := uint16(0)
		pageOffset := entry.Offset
		for {
			numberOfBytesToRead := min(entry.Len-totalRead, h.PageSize-pageOffset)
			totalRead += numberOfBytesToRead
			if pageIDIdx != -1 {
				usedPageIdx[pageIDIdx] = true
			}

			if totalRead == entry.Len {
				break
			}
			pageIDIdx++
			pageOffset = 0
		}
	}

	unusedPages := make([]int, 0)
	for idx, isUsed := range usedPageIdx {
		if !isUsed {
			unusedPages = append(unusedPages, idx)
		}
	}

	for _, pageIdx := range unusedPages {
		if err := h.Pager.FreePage(txn, header.PageIDArr[pageIdx]); err != nil {
			return err
		}
	}

	h.freePagesAtIdx(header, unusedPages...)

	return nil
}

func (h *Heap) logicalOffset(header *Header, slotIdx int) int {
	entry := header.SlotArr[slotIdx]
	for i, pid := range header.PageIDArr {
		if pid == entry.PageID {
			return int(entry.Offset) + (i * int(h.PageSize))
		}
	}

	common.AssertFail("page is not in pageID array")
	return 0
}

func (h *Heap) pageIDIdx(header *Header, pageID uint64) int {
	if h.HeadPageID == pageID {
		return -1
	}

	for i, u := range header.PageIDArr {
		if u == pageID {
			return i
		}
	}

	common.AssertFail("page is not in pageID array")
	return 0
}

func (h *Heap) freePagesAtIdx(header *Header, at ...int) {
	// TODO: free pages
	newArr := make([]uint64, 0)
	for i, pid := range header.PageIDArr {
		if common.Contains(at, i) {
			continue
		}

		newArr = append(newArr, pid)
	}

	header.PageIDArr = newArr
}

func (h *Heap) nextPage(header *Header, pageID uint64) uint64 {
	if pageID == h.HeadPageID {
		return header.PageIDArr[0]
	}

	for i, curr := range header.PageIDArr {
		if curr == pageID {
			if len(header.PageIDArr) > i+1 {
				return header.PageIDArr[i+1]
			}

			return 0
		}
	}

	return 0
}

func (h *Heap) getHeader(txn transaction.Transaction) (*Header, error) {
	hp, err := h.Pager.GetPageToRead(txn, h.HeadPageID)
	if err != nil {
		return nil, err
	}

	defer hp.Release(false)

	return readHeader(hp.GetData()), nil
}

func (h *Heap) getSlotEntryAt(txn transaction.Transaction) (*Header, error) {
	hp, err := h.Pager.GetPageToRead(txn, h.HeadPageID)
	if err != nil {
		return nil, err
	}

	defer hp.Release(false)

	return readHeader(hp.GetData()), nil
}

func (h *Heap) setHeader(txn transaction.Transaction, header *Header) error {
	hp, err := h.Pager.GetPageToWrite(txn, h.HeadPageID)
	if err != nil {
		return err
	}

	defer hp.Release(true)

	return hp.CopyAt(txn, 0, header.asBytes())
}
