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
	WritePage(data []byte, pageId uint64) error
	WritePages(data [][]byte, pageId []uint64) error
	ReadPage(pageId uint64) ([]byte, error)
	NewPage() (pageId uint64)
	FreePage(pageId uint64)
	GetCatalogPID() uint64
	SetCatalogPID(pid uint64)
	Close() error

	GetLogWriter() io.Writer
}

type LogFile interface {
	io.Seeker
	io.ReadCloser
	io.Writer
}

const PageSize int = 4096

// FlushInstantly should normally be set to true. If it is false then data might be lost even after a successful write
// operation when power loss occurs before os flushes its io buffers. But when it is false, one thread tests runs faster
// thanks to io scheduling of os, so for development it could be set to false. Setting it to false should not change
// the validity of any tests unless a test is simulating a power loss.
const FlushInstantly bool = false

type Manager struct {
	file        *os.File
	filename    string
	logFile     LogFile
	logFileName string
	lastPageId  uint64
	mu          sync.Mutex
	serializer  IHeaderSerializer
	header      *header
}

func NewDiskManager(file string) (IDiskManager, bool, error) {
	d := Manager{}
	d.serializer = jsonSerializer{}
	d.filename = file
	d.logFileName = file + ".log"

	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	lf, err := os.OpenFile(d.logFileName, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

	d.logFile = lf
	d.file = f
	stats, _ := f.Stat()

	filesize := stats.Size()
	log.Printf("db is initalizing, file size is %d \n", filesize)

	if filesize == 0 {
		// if it is a new db file
		d.lastPageId = 1 // first page is reserved, so start from 1
		d.initHeader()
		return &d, true, nil
	} else {
		d.lastPageId = uint64((int(filesize) / PageSize) - 1)
	}

	return &d, false, nil
}

func (d *Manager) WritePage(data []byte, pageId uint64) error {
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

func (d *Manager) WritePages(pages [][]byte, pageIds []uint64) error {
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

func (d *Manager) ReadPage(pageId uint64) ([]byte, error) {
	_, err := d.file.Seek((int64(PageSize))*int64(pageId), io.SeekStart)
	if err != nil {
		return []byte{}, err
	}

	// TODO: update this method to receive destination bytes instead of dynamically allocating
	// and returning it.
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

func (d *Manager) NewPage() (pageId uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// if pop free list is successful return popped page
	if p := d.popFreeList(); p != 0 {
		return p
	}

	// else allocate new page
	d.lastPageId++
	return d.lastPageId
}

// FreePage appends page with given id to freelist and sets it as tail.
func (d *Manager) FreePage(pageId uint64) {
	d.mu.Lock()
	d.mu.Unlock()
	h := d.getHeader()

	// if free list is empty
	if h.freeListHead == 0 {
		h.freeListHead = pageId
		h.freeListTail = pageId
		d.setHeader(h)
		return
	}

	// freed page may not be synced to file just yet. in that case ReadPage returns io.EOF and for the consistence of
	// freelist it needs to be written to disk. Hence, empty bytes are initialized and page is flushed.
	data, err := d.ReadPage(h.freeListTail)
	if err == io.EOF {
		data = make([]byte, PageSize, PageSize)
	} else if err != nil {
		panic(err)
	}

	binary.BigEndian.PutUint64(data, pageId)
	if err := d.WritePage(data, h.freeListTail); err != nil {
		panic(err)
	}

	h.freeListTail = pageId
	d.setHeader(h)
}

func (d *Manager) Close() error {
	if err := d.logFile.Close(); err != nil {
		return err
	}
	return d.file.Close()
}

func (d *Manager) GetCatalogPID() uint64 {
	d.mu.Lock()
	h := d.getHeader()
	d.mu.Unlock()
	return h.catalogPID
}

func (d *Manager) SetCatalogPID(pid uint64) {
	d.mu.Lock()
	h := d.getHeader()
	h.catalogPID = pid
	d.setHeader(h)
	d.mu.Unlock()
}

func (d *Manager) GetLogWriter() io.Writer {
	return d.logFile
}

func (d *Manager) writePages(pages [][]byte, startingPageId uint64) error {
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

func (d *Manager) popFreeList() (pageId uint64) {
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

	data, err := d.ReadPage(h.freeListHead)
	if err != nil {
		panic(err)
	}

	h.freeListHead = binary.BigEndian.Uint64(data)
	d.setHeader(h)
	return
}

func (d *Manager) getHeader() header {
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

func (d *Manager) setHeader(h header) {
	d.header = &h
	page := make([]byte, PageSize, PageSize)
	writeHeader(h, page)
	if err := d.WritePage(page, 0); err != nil {
		panic(err)
	}
}

func (d *Manager) initHeader() {
	d.setHeader(header{
		freeListHead: 0,
		freeListTail: 0,
		catalogPID:   0,
	})
}

type header struct {
	freeListHead uint64
	freeListTail uint64
	catalogPID   uint64
}

func readHeader(data []byte) header {
	return header{
		freeListHead: binary.BigEndian.Uint64(data),
		freeListTail: binary.BigEndian.Uint64(data[8:]),
		catalogPID:   binary.BigEndian.Uint64(data[16:]),
	}
}

func writeHeader(h header, dest []byte) {
	binary.BigEndian.PutUint64(dest, h.freeListHead)
	binary.BigEndian.PutUint64(dest[8:], h.freeListTail)
	binary.BigEndian.PutUint64(dest[16:], h.catalogPID)
}
