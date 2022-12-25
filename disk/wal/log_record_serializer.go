package wal

import (
	"encoding/binary"
	"helin/disk/pages"
	"io"
)

type LogRecordSerializer interface {
	Serialize(r *LogRecord, writer io.Writer)
	Size(r *LogRecord) int

	// Deserialize reads from src and constructs a LogRecord. Deserialize should not change the content of the src.
	Deserialize(src io.Reader) (*LogRecord, int)
}

var _ LogRecordSerializer = &DefaultLogRecordSerializer{}

type DefaultLogRecordSerializer struct {
	area []byte
}

func (d *DefaultLogRecordSerializer) Serialize(r *LogRecord, writer io.Writer) {
	d.area = append(d.area, byte(r.t))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.txnID))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.lsn))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.prevLsn))
	d.area = binary.BigEndian.AppendUint16(d.area, r.idx)
	d.area = binary.BigEndian.AppendUint64(d.area, r.pageID)
	d.area = binary.BigEndian.AppendUint64(d.area, r.prevPageID)
	d.area = binary.BigEndian.AppendUint16(d.area, uint16(len(r.payload)))
	d.area = binary.BigEndian.AppendUint16(d.area, uint16(len(r.payload)))
	n, err := writer.Write(d.area)
	if err != nil {
		panic(err)
	}
	if n != len(d.area) {
		panic("short write")
	}

	d.area = d.area[:0]

	n, err = writer.Write(r.payload)
	if err != nil {
		panic(err)
	}
	if n != len(r.payload) {
		panic("short write")
	}

	n, err = writer.Write(r.oldPayload)
	if err != nil {
		panic(err)
	}
	if n != len(r.oldPayload) {
		panic("short write")
	}
}

func (d *DefaultLogRecordSerializer) Size(r *LogRecord) int {
	size := LogRecordInlineSize
	if len(r.oldPayload) > 0 {
		// +2 is for writing length of the payload
		size += len(r.oldPayload) + 2
	}
	if len(r.payload) > 0 {
		size += len(r.payload) + 2
	}
	return size
}

func (d *DefaultLogRecordSerializer) Deserialize(src io.Reader) (*LogRecord, int) {
	d.area = d.area[:LogRecordInlineSize+2+2]
	n, err := src.Read(d.area)
	if err != nil {
		panic(err)
	}
	if n != len(d.area) {
		panic("short read")
	}
	res := LogRecord{}
	res.t = LogRecordType(d.area[0])
	res.txnID = TxnID(binary.BigEndian.Uint64(d.area[1:]))
	res.lsn = pages.LSN(binary.BigEndian.Uint64(d.area[9:]))
	res.prevLsn = pages.LSN(binary.BigEndian.Uint64(d.area[17:]))
	res.idx = binary.BigEndian.Uint16(d.area[25:])
	res.pageID = binary.BigEndian.Uint64(d.area[27:])
	res.prevPageID = binary.BigEndian.Uint64(d.area[35:])
	lenp := binary.BigEndian.Uint16(d.area[43:])
	lenop := binary.BigEndian.Uint16(d.area[45:])

	payload := make([]byte, lenp)
	oldpayload := make([]byte, lenop)

	n, err = src.Read(payload)
	if err != nil {
		panic(err)
	}
	if n != len(payload) {
		panic("short read")
	}

	n, err = src.Read(oldpayload)
	if err != nil {
		panic(err)
	}
	if n != len(oldpayload) {
		panic("short read")
	}

	res.payload = payload
	res.oldPayload = oldpayload

	return &res, 0
}
