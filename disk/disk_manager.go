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
}

const PageUsableSize int = PageSize - 16 // TODO IMPORTANT: move this
const PageSize int = 4096 * 2

type Manager struct {
	file       *os.File
	filename   string
	lastPageId uint64
	globalMu   sync.Mutex
	seekMu     sync.Mutex
	serializer IHeaderSerializer
	header     *header

	// fsync should normally be set to true. If it is false then data might be lost even after a successful write
	// operation when power loss occurs before os flushes its io buffers. But when it is false, one thread tests runs faster
	// thanks to io scheduling of os, so for development it could be set to false. Setting it to false should not change
	// the validity of any tests unless a test is simulating a power loss.
	fsync bool

	count int32
}

func NewDiskManager(file string, fsync bool) (*Manager, bool, error) {
	d := Manager{fsync: fsync}
	d.serializer = jsonSerializer{}
	d.filename = file

	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, false, err
	}

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

func (d *Manager) WritePage2(data []byte, pageId uint64) error {
	d.seekMu.Lock()
	defer d.seekMu.Unlock()

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

	if d.fsync {
		if err := d.file.Sync(); err != nil {
			panic(err)
		}
	}

	return nil
}

func (d *Manager) ReadPage2(pageId uint64, dest []byte) error {
	d.seekMu.Lock()
	defer d.seekMu.Unlock()

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

func (d *Manager) WritePage(data []byte, pageId uint64) error {
	n, err := d.file.WriteAt(data, (int64(PageSize))*int64(pageId))
	if err != nil {
		return err
	}
	if n != PageSize {
		panic("written bytes are not equal to page size")
	}

	if d.fsync {
		if err := d.file.Sync(); err != nil {
			panic(err)
		}
	}

	return nil
}

func (d *Manager) ReadPage(pageId uint64, dest []byte) error {
	_ = dest[PageSize-1] // bound check

	n, err := d.file.ReadAt(dest, (int64(PageSize))*int64(pageId))
	if err != nil {
		return err
	}
	if n != PageSize {
		panic(fmt.Sprintf("Partial page encountered this should not happen. Page id: %d", pageId))
	}

	return nil
}

func (d *Manager) NewPage() (pageId uint64) {
	d.globalMu.Lock()
	defer d.globalMu.Unlock()

	d.lastPageId++
	return d.lastPageId
}

func (d *Manager) Close() error {
	return d.file.Close()
}

func (d *Manager) GetCatalogPID() uint64 {
	d.globalMu.Lock()
	h := d.getHeader()
	d.globalMu.Unlock()
	return h.catalogPID
}

func (d *Manager) SetCatalogPID(pid uint64) {
	d.globalMu.Lock()
	h := d.getHeader()
	h.catalogPID = pid
	d.setHeader(h)
	d.globalMu.Unlock()
}

func (d *Manager) getHeader() header {
	if d.header != nil {
		return *d.header
	}

	var data = make([]byte, PageSize)
	if err := d.ReadPage(0, data); err == io.EOF {
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
