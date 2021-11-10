package disk

import (
	"fmt"
	"io"
	"os"
	"sync"
)

type IDiskManager interface {
	WritePage(data []byte, pageId int) error
	ReadPage(pageId int) ([]byte, error)
	NewPage() (pageId int)
	FreePage(pageId int)
}

const PageSize int = 4096

type DiskManager struct {
	file       *os.File
	filename   string
	lastPageId int
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

	return &d, nil
}

func (d *DiskManager) WritePage(data []byte, pageId int) error {
	_, err := d.file.Seek((int64(PageSize))*int64(pageId), io.SeekStart)

	if err != nil {
		return err
	}

	write := data[:]
	d.file.Write(write)
	d.file.Sync()

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
