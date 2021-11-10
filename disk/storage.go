package disk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
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

type DiskManager struct {
	file       *os.File
	filename   string
	lastPageId int
	freePages  []int
	mu         sync.Mutex
	serializer IHeaderSerializer
}

func checkErr(err error) {
	if err != nil {
		fmt.Println(err.Error())
		panic(err)
	}
}

func NewDiskManager(file string) (IDiskManager, error) {
	d := DiskManager{}
	d.serializer = jsonSerializer{}
	d.filename = file
	f, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, os.ModePerm)
	checkErr(err)
	d.file = f
	stats, _ := f.Stat()

	filesize := stats.Size()
	fmt.Printf("file size is %d \n", filesize)

	d.lastPageId = (int(filesize) / PageSize) - 1
	if d.lastPageId == -1 {
		d.lastPageId = 1 // bplus tree pointer cannot be zero hence start from 1
	}
	return &d, nil
}

func (d *DiskManager) WritePage(data []byte, pageId int) error {
	_, err := d.file.Seek((int64(PageSize))*int64(pageId), io.SeekStart)

	if err != nil {
		return err
	}

	write := data[:]
	n, err := d.file.Write(write)
	if err != nil {
		panic(err.Error())
	}
	if n != PageSize {
		panic("written bytes are not equal to pagesize")
	}
	err = d.file.Sync()
	if err != nil {
		panic(err.Error())
	}

	return nil
}

func (d *DiskManager) WritePages(datas [][]byte, pageIds []int) error {
	if len(datas) != len(pageIds) {
		return errors.New("number of data pages is not equal to number of pageIds")
	}

	sort.Slice(datas, func(i, j int) bool {
		return pageIds[i] < pageIds[j]
	})

	sort.Slice(pageIds, func(i, j int) bool {
		return pageIds[i] < pageIds[j]
	})

	st := 0
	for i, currPageId := range pageIds {
		if i == len(pageIds)-1 {
			err := d._writePages(datas[st:], pageIds[st])
			if err != nil {
				return err
			}
			break
		}
		nextPageId := pageIds[i+1]

		if nextPageId > currPageId+1 {
			err := d._writePages(datas[st:i+1], pageIds[st])
			if err != nil {
				return err
			}
			st = i + 1
		}
	}
	return nil
}

func (d *DiskManager) _writePages(data [][]byte, startingPageId int) error {
	_, err := d.file.Seek((int64(PageSize))*int64(startingPageId), io.SeekStart)

	if err != nil {
		return err
	}

	write := make([]byte, 0, PageSize*len(data))
	for _, datum := range data {
		write = append(write, datum...)
	}

	n, err := d.file.Write(write)
	if err != nil {
		panic(err.Error())
	}
	if n != PageSize*len(data) {
		panic("written bytes are not equal to pagesize")
	}
	if err != nil {
		panic(err.Error())
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

	d.lastPageId++
	return d.lastPageId
}

func (d *DiskManager) FreePage(pageId int) {
	// TODO: noop for now
}

func (d *DiskManager) Close() error {
	return d.file.Close()
}

func (d *DiskManager) InitFreePages(newFile bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if newFile {
		d.freePages = make([]int, 0)
		d.syncFreePages()
		return
	}

	// read first page which should keep binary serialized version for list of pageIds
	data, err := d.ReadPage(0)
	checkErr(err)

	// iterate until seen null
	i := 0
	for ; i < len(data); i++ {
		if data[i] == '\000' {
			break
		}
	}

	binary.Read(bytes.NewReader(data[:i]), binary.BigEndian, &d.freePages)

	d.syncFreePages()
}

func (d *DiskManager) syncFreePages() {
	buf := bytes.Buffer{}
	binary.Write(&buf, binary.BigEndian, d.freePages)
	bin := buf.Bytes()
	if len(bin) > PageSize {
		// TODO: add overflow pages
		panic("free pages cannot be synced since it is more than a page size")
	}
	err := d.WritePage(buf.Bytes(), 0)
	checkErr(err)
}

func (d *DiskManager) SetFreePage(pageId int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.freePages = append(d.freePages, pageId)

	d.syncFreePages()
}

func (d *DiskManager) UnsetFreePage(pageId int) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	found := 0
	i := 0
	for ; i < len(d.freePages); i++ {
		if d.freePages[i] == pageId {
			found = 1
			break
		}
	}
	if found == 0 {
		return errors.New("page was not set as free hence cannot be unset")
	}

	if i < len(d.freePages)-1 {
		copy(d.freePages[i:], d.freePages[i+1:])
	}
	d.freePages = d.freePages[:len(d.freePages)-1]

	d.syncFreePages()
	return nil
}
