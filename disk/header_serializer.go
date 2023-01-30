package disk

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

// NOTE: whole content of this file is only used while debugging.

type PageHeader struct {
	IsUsed bool
}

var x, _ = json.Marshal(PageHeader{true})
var pageHeaderSize = len(x) //binary.Size(PageHeader{true}) TODO: should be fixed size

type IHeaderSerializer interface {
	encodePageHeader(pageHeader PageHeader) []byte
	readPageHeader(page []byte) PageHeader
}

// binary serializer implementation
type binarySerializer struct{}

func (r binarySerializer) readPageHeader(page []byte) PageHeader {
	header := page[:pageHeaderSize]

	reader := bytes.NewReader(header)

	pageHeader := PageHeader{}
	binary.Read(reader, binary.LittleEndian, &pageHeader)

	return pageHeader
}

func (r binarySerializer) encodePageHeader(pageHeader PageHeader) []byte {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.LittleEndian, pageHeader)

	if err != nil {
		panic(err.Error())
	}

	b := buf.Bytes()
	return b
}

// json serializer implementation
type jsonSerializer struct{}

func (r jsonSerializer) readPageHeader(page []byte) PageHeader {
	var res PageHeader
	header := page[:pageHeaderSize]
	json.Unmarshal(header, &res)
	println(res.IsUsed)
	return res
}

func (r jsonSerializer) encodePageHeader(pageHeader PageHeader) []byte {
	b, _ := json.Marshal(pageHeader)
	return b
}
