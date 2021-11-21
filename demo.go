package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

type IDiskManager interface {
	WritePage(data [PageSize]byte, pageId int) error
	ReadPage(pageId int) ([]byte, error)
	NewPage() (pageId int)
	FreePage(pageId int)
}

const PageSize int = 4096

type PageHeader struct {
	IsUsed bool
}

var x, _ = json.Marshal(PageHeader{true})
var pageHeaderSize = len(x) //binary.Size(PageHeader{true}) TODO: should be fixed size

type DiskManager struct {
	file       *os.File
	filename   string
	lastPageId int
	mu         sync.Mutex
	serializer IHeaderSerializer
}

func log(err error) {
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
	log(err)
	d.file = f
	stats, _ := f.Stat()

	filesize := stats.Size()
	fmt.Printf("file size is %d \n", filesize)

	d.lastPageId = (int(filesize) / PageSize) - 1

	return &d, nil
}

func (d *DiskManager) WritePage(data [PageSize]byte, pageId int) error {
	_, err := d.file.Seek((int64(PageSize)+int64(pageHeaderSize))*int64(pageId), io.SeekStart)

	if err != nil {
		return err
	}

	pageHeader := PageHeader{true}
	b := d.serializer.encodePageHeader(pageHeader)

	write := append(b, data[:]...)
	d.file.Write(write)
	d.file.Sync()

	return nil
}

func (d *DiskManager) ReadPage(pageId int) ([]byte, error) {
	_, err := d.file.Seek((int64(PageSize)+int64(pageHeaderSize))*int64(pageId), io.SeekStart)

	if err != nil {
		return []byte{}, err
	}

	var data = make([]byte, pageHeaderSize+PageSize)
	n, err := d.file.Read(data[:])

	if err != nil {
		return []byte{}, err
	}

	pageHeader := d.serializer.readPageHader(data)

	if pageHeader.IsUsed == false {
		panic("page is not used")
	}

	if n != PageSize+pageHeaderSize {
		panic(fmt.Sprintf("Partial page encountered this should not happen. Page id: %d", pageId))
	}

	return data[pageHeaderSize:], nil
}

func (d *DiskManager) NewPage() (pageId int) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lastPageId++
	return d.lastPageId
}

func (d *DiskManager) FreePage(pageId int) {
	b := d.serializer.encodePageHeader(PageHeader{false})
	d.file.Seek((int64(PageSize)+int64(pageHeaderSize))*int64(pageId), io.SeekStart)
	d.file.Write(b)
	d.file.Sync()
}

type demostruct struct {
	Num int
	Val string
}

func main() {
	d, _ := NewDiskManager("sa")
	page := d.NewPage()
	fmt.Println("lalalla")
	fmt.Println(fmt.Sprintf("Writing to page %d", page))
	x := demostruct{Num: 45, Val: "selam"}
	json, _ := json.Marshal(x)
	var data [4096]byte

	copy(data[:], json)

	d.WritePage([PageSize]byte(data), page)
	read, _ := d.ReadPage(8)
	fmt.Print(string(read[:]))
	d.FreePage(12)
}
