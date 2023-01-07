package wal

import (
	"encoding/binary"
	"encoding/json"
	"helin/disk/pages"
	"helin/transaction"
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
	d.area = append(d.area, byte(r.T))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.TxnID))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.Lsn))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.PrevLsn))
	d.area = binary.BigEndian.AppendUint16(d.area, r.Idx)
	d.area = binary.BigEndian.AppendUint64(d.area, r.PageID)
	d.area = binary.BigEndian.AppendUint64(d.area, r.PrevPageID)
	d.area = binary.BigEndian.AppendUint16(d.area, uint16(len(r.Payload)))
	d.area = binary.BigEndian.AppendUint16(d.area, uint16(len(r.Payload)))
	n, err := writer.Write(d.area)
	if err != nil {
		panic(err)
	}
	if n != len(d.area) {
		panic("short write")
	}

	d.area = d.area[:0]

	n, err = writer.Write(r.Payload)
	if err != nil {
		panic(err)
	}
	if n != len(r.Payload) {
		panic("short write")
	}

	n, err = writer.Write(r.OldPayload)
	if err != nil {
		panic(err)
	}
	if n != len(r.OldPayload) {
		panic("short write")
	}
}

func (d *DefaultLogRecordSerializer) Size(r *LogRecord) int {
	size := LogRecordInlineSize
	if len(r.OldPayload) > 0 {
		// +2 is for writing length of the payload
		size += len(r.OldPayload) + 2
	}
	if len(r.Payload) > 0 {
		size += len(r.Payload) + 2
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
	res.T = LogRecordType(d.area[0])
	res.TxnID = transaction.TxnID(binary.BigEndian.Uint64(d.area[1:]))
	res.Lsn = pages.LSN(binary.BigEndian.Uint64(d.area[9:]))
	res.PrevLsn = pages.LSN(binary.BigEndian.Uint64(d.area[17:]))
	res.Idx = binary.BigEndian.Uint16(d.area[25:])
	res.PageID = binary.BigEndian.Uint64(d.area[27:])
	res.PrevPageID = binary.BigEndian.Uint64(d.area[35:])
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

	res.Payload = payload
	res.OldPayload = oldpayload

	return &res, 0
}

var _ LogRecordSerializer = &JsonLogRecordSerializer{}

type JsonLogRecordSerializer struct {
}

func (j *JsonLogRecordSerializer) Serialize(r *LogRecord, writer io.Writer) {
	if err := json.NewEncoder(writer).Encode(r); err != nil {
		panic(err)
	}
}

func (j *JsonLogRecordSerializer) Size(r *LogRecord) int {
	//TODO implement me
	panic("implement me")
}

func (j *JsonLogRecordSerializer) Deserialize(src io.Reader) (*LogRecord, int) {
	record := LogRecord{}
	if err := json.NewDecoder(src).Decode(&record); err != nil {
		panic(err)
	}

	return &record, 0 // NOTE: TODO: returns 0
}
