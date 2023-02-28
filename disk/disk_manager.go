package disk

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

type IDiskManager interface {
	WritePage(data []byte, pageId uint64) error
	ReadPage(pageId uint64, dest []byte) error
	NewPage() (pageId uint64)
	GetCatalogPID() uint64
	SetCatalogPID(pid uint64)
	Close() error

	GetLogWriter() io.Writer
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
	logFile     *os.File
	logFileName string
	lastPageId  uint64
	mu          sync.Mutex
	serializer  IHeaderSerializer
	header      *header
}

func NewDiskManager(file string) (*Manager, bool, error) {
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
		d.lastPageId = 2 // first two page is reserved, so start from 2
		//d.initHeader()
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

func (d *Manager) ReadPage(pageId uint64, dest []byte) error {
	_ = dest[PageSize-1] // bound check
	_, err := d.file.Seek((int64(PageSize))*int64(pageId), io.SeekStart)
	if err != nil {
		return err
	}

	n, err := d.file.Read(dest)
	if err != nil {
		return err
	}
	if n != PageSize {
		panic(fmt.Sprintf("Partial page encountered this should not happen. Page id: %d", pageId))
	}

	return nil
}

func (d *Manager) NewPage() (pageId uint64) {
	d.mu.Lock()
	defer d.mu.Unlock()

	//// if pop free list is successful return popped page
	//if p := d.popFreeList(); p != 0 {
	//	return p
	//}

	// else allocate new page
	d.lastPageId++
	return d.lastPageId
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
	return &SyncWriter{d.logFile}
}

func (d *Manager) getHeader() header {
	if d.header != nil {
		return *d.header
	}

	var data = make([]byte, PageSize)
	if err := d.ReadPage(0, nil); err == io.EOF {
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
		catalogPID: 0,
	})
}

type header struct {
	catalogPID uint64
}

func readHeader(data []byte) header {
	return header{
		catalogPID: binary.BigEndian.Uint64(data),
	}
}

func writeHeader(h header, dest []byte) {
	binary.BigEndian.PutUint64(dest, h.catalogPID)
}

// SyncWriter calls fsync after each write, so it guarantees every write is persisted to disk. It is useful for
// log writer.
type SyncWriter struct {
	*os.File
}

func (r *SyncWriter) Write(d []byte) (int, error) {
	n, err := r.File.Write(d)
	if err := r.File.Sync(); err != nil {
		return n, err
	}
	return n, err
}
