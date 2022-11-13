package disk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"sync"
)

type IDiskManager interface {
	WritePage(data []byte, pageId int) error
	WritePages(data [][]byte, pageId []int) error
	ReadPage(pageId int) ([]byte, error)
	NewPage() (pageId int)
	FreePage(pageId int)
	Close() error
}

const PageSize int = 4096

// FlushInstantly should normally be set to true. If it is false then data might be lost even after a successful write
// operation when power loss occurs before os flushes its io buffers. But when it is false, one thread tests runs faster
// thanks to io scheduling of os, so for development it could be set to false. Setting it to false should not change
// the validity of any tests unless a test is simulating a power loss.
const FlushInstantly bool = false

type DiskManager struct {
	file       *os.File
	filename   string
	lastPageId int
	mu         sync.Mutex
	serializer IHeaderSerializer
	header     *header
}

func NewDiskManager(file string) (IDiskManager, error) {
	d := DiskManager{}
	d.serializer = jsonSerializer{}
	d.filename = file

	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	d.file = f
	stats, _ := f.Stat()

	filesize := stats.Size()
	log.Printf("db is initalizing, file size is %d \n", filesize)

	d.lastPageId = (int(filesize) / PageSize) - 1
	if d.lastPageId == -1 {
		// if it is a new db file
		d.lastPageId = 1 // first page is reserved, so start from 1

		d.initHeader()
	}

	return &d, nil
}

func (d *DiskManager) WritePage(data []byte, pageId int) error {
	_, err := d.file.Seek((int64(PageSize))*int64(pageId), io.SeekStart)
	if err != nil {
		return err
	}

	n, err := d.file.Write(data)
	if err != nil {
		return err
	}
	if n != PageSize {
		panic("written bytes are not equal to page size")
	}

	if FlushInstantly {
		if err := d.file.Sync(); err != nil {
			panic(err)
		}
	}

	return nil
}

func (d *DiskManager) WritePages(pages [][]byte, pageIds []int) error {
	if len(pages) != len(pageIds) {
		return errors.New("number of data pages is not equal to number of pageIds")
	}

	sort.Slice(pages, func(i, j int) bool {
		return pageIds[i] < pageIds[j]
	})

	sort.Slice(pageIds, func(i, j int) bool {
		return pageIds[i] < pageIds[j]
	})

	st := 0
	for i, currPageId := range pageIds {
		if i == len(pageIds)-1 {
			err := d.writePages(pages[st:], pageIds[st])
			if err != nil {
				return err
			}
			break
		}
		nextPageId := pageIds[i+1]

		if nextPageId > currPageId+1 {
			err := d.writePages(pages[st:i+1], pageIds[st])
			if err != nil {
				return err
			}
			st = i + 1
		}
	}
	return nil
}

func (d *DiskManager) ReadPage(pageId int) ([]byte, error) {
	_, err := d.file.Seek((int64(PageSize))*int64(pageId), io.SeekStart)
	if err != nil {
		return []byte{}, err
	}

	var data = make([]byte, PageSize)

	n, err := d.file.Read(data[:])
	if err != nil {
		return []byte{}, err
	}
	if n != PageSize {
		panic(fmt.Sprintf("Partial page encountered this should not happen. Page id: %d", pageId))
	}

	return data[:], nil
}

func (d *DiskManager) NewPage() (pageId int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// if pop free list is successful return popped page
	if p := d.popFreeList(); p != 0 {
		return int(p)
	}

	// else allocate new page
	d.lastPageId++
	return d.lastPageId
}

// FreePage appends page with given id to freelist and sets it as tail.
func (d *DiskManager) FreePage(pageId int) {
	d.mu.Lock()
	d.mu.Unlock()
	h := d.getHeader()

	// if free list is empty
	if h.freeListHead == 0 {
		h.freeListHead = uint64(pageId)
		h.freeListTail = uint64(pageId)
		d.setHeader(h)
		return
	}

	// freed page may not be synced to file just yet. in that case ReadPage returns io.EOF and for the consistence of
	// freelist it needs to be written to disk. Hence, empty bytes are initialized and page is flushed.
	data, err := d.ReadPage(int(h.freeListTail))
	if err == io.EOF {
		data = make([]byte, PageSize, PageSize)
	} else if err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(data, uint64(pageId))
	if err := d.WritePage(data, int(h.freeListTail)); err != nil {
		panic(err)
	}

	h.freeListTail = uint64(pageId)
	d.setHeader(h)
}

func (d *DiskManager) Close() error {
	return d.file.Close()
}

func (d *DiskManager) writePages(pages [][]byte, startingPageId int) error {
	_, err := d.file.Seek((int64(PageSize))*int64(startingPageId), io.SeekStart)

	if err != nil {
		return err
	}

	write := make([]byte, 0, PageSize*len(pages))
	for _, datum := range pages {
		write = append(write, datum...)
	}

	n, err := d.file.Write(write)
	if err != nil {
		panic(err.Error())
	}
	if n != PageSize*len(pages) {
		panic("written bytes are not equal to page size")
	}
	if err != nil {
		panic(err.Error())
	}

	return nil
}

func (d *DiskManager) popFreeList() (pageId uint64) {
	// if list is empty return 0
	h := d.getHeader()
	if h.freeListHead == 0 {
		return 0
	}

	// if there is only on entry in free list return that and set head and tail to 0
	if h.freeListHead == h.freeListTail {
		pageId = h.freeListHead
		h.freeListHead, h.freeListTail = 0, 0
		d.setHeader(h)
		return
	}

	// else pop head, read new head and update header
	pageId = h.freeListHead

	data, err := d.ReadPage(int(h.freeListHead))
	if err != nil {
		panic(err)
	}

	h.freeListHead = binary.BigEndian.Uint64(data)
	d.setHeader(h)
	return
}

func (d *DiskManager) getHeader() header {
	if d.header != nil {
		return *d.header
	}

	data, err := d.ReadPage(0)
	if err == io.EOF {
		d.initHeader()
		data = make([]byte, PageSize, PageSize)
	} else if err != nil {
		panic(err)
	}

	h := readHeader(data)
	d.header = &h
	return h
}

func (d *DiskManager) setHeader(h header) {
	d.header = &h
	page := make([]byte, PageSize, PageSize)
	writeHeader(h, page)
	if err := d.WritePage(page, 0); err != nil {
		panic(err)
	}
}

func (d *DiskManager) initHeader() {
	d.setHeader(header{
		freeListHead: 0,
		freeListTail: 0,
	})
}

type header struct {
	freeListHead uint64
	freeListTail uint64
}

func readHeader(data []byte) header {
	return header{
		freeListHead: binary.BigEndian.Uint64(data),
		freeListTail: binary.BigEndian.Uint64(data[8:]),
	}
}

func writeHeader(h header, dest []byte) {
	binary.BigEndian.PutUint64(dest, h.freeListHead)
	binary.BigEndian.PutUint64(dest[8:], h.freeListTail)
}
