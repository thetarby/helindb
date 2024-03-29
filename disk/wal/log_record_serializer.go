package wal

import (
	"encoding/binary"
	"errors"
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
	"io"
)

var ErrShortRead = errors.New("short read")

type LogRecordSerializer interface {
	Serialize(r *LogRecord, writer LogWriter)
	Size(r *LogRecord) int

	// Deserialize reads from src and constructs a LogRecord. Deserialize should not change the content of the src.
	Deserialize(src io.Reader) (*LogRecord, int, error)
}

var _ LogRecordSerializer = &DefaultLogRecordSerializer{}

type DefaultLogRecordSerializer struct {
	area []byte
}

func NewDefaultLogRecordSerializer() *DefaultLogRecordSerializer {
	return &DefaultLogRecordSerializer{
		area: make([]byte, 0, 100),
	}
}

func (d *DefaultLogRecordSerializer) Serialize(r *LogRecord, writer LogWriter) {
	d.area = d.area[:0]
	d.area = append(d.area, byte(r.T))
	if r.T == TypeCheckpointBegin || r.T == TypeCheckpointEnd {
		d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.Lsn))

		n, err := writer.Write(d.area, r.Lsn)
		if err != nil {
			panic(err)
		}
		if n != len(d.area) {
			panic("short write")
		}

		serUint64(writer, r.Actives, r.Lsn)
		return
	}
	if common.OneOf(r.T, TypeCommit, TypeTxnBegin, TypeAbort) {
		d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.TxnID))
		d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.Lsn))

		n, err := writer.Write(d.area, r.Lsn)
		if err != nil {
			panic(err)
		}
		if n != len(d.area) {
			panic("short write")
		}

		if r.T == TypeCommit {
			serUint64(writer, r.FreedPages, r.Lsn)
		}

		return
	}

	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.TxnID))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.Lsn))
	d.area = binary.BigEndian.AppendUint64(d.area, uint64(r.PrevLsn))
	d.area = binary.BigEndian.AppendUint16(d.area, r.Idx)
	d.area = binary.BigEndian.AppendUint64(d.area, r.PageID)
	d.area = binary.BigEndian.AppendUint64(d.area, r.PrevPageID)
	d.area = binary.BigEndian.AppendUint16(d.area, uint16(len(r.Payload)))
	d.area = binary.BigEndian.AppendUint16(d.area, uint16(len(r.OldPayload)))
	n, err := writer.Write(d.area, r.Lsn)
	if err != nil {
		panic(err)
	}
	if n != len(d.area) {
		panic("short write")
	}

	d.area = d.area[:0]

	n, err = writer.Write(r.Payload, r.Lsn)
	if err != nil {
		panic(err)
	}
	if n != len(r.Payload) {
		panic("short write")
	}

	n, err = writer.Write(r.OldPayload, r.Lsn)
	if err != nil {
		panic(err)
	}
	if n != len(r.OldPayload) {
		panic("short write")
	}
}

func (d *DefaultLogRecordSerializer) Deserialize(r io.Reader) (*LogRecord, int, error) {
	src := common.NewStatReader(r)
	d.area = d.area[:LogRecordInlineSize+2+2]

	n, err := src.Read(d.area[:1])
	if err != nil {
		return nil, src.TotalRead, err
	}

	if LogRecordType(d.area[0]) == TypeCheckpointEnd || LogRecordType(d.area[0]) == TypeCheckpointBegin {
		var lsn pages.LSN
		if err := read(src, &lsn); err != nil {
			return nil, src.TotalRead, err
		}

		txnList, err := deSerUint64[transaction.TxnID](src)
		if err != nil {
			return nil, src.TotalRead, err
		}

		return &LogRecord{T: LogRecordType(d.area[0]), Lsn: lsn, Actives: txnList}, src.TotalRead, nil
	}

	if common.OneOf(LogRecordType(d.area[0]), TypeCommit, TypeTxnBegin, TypeAbort) {
		var t uint64
		if err := read(src, &t); err != nil {
			return nil, src.TotalRead, err
		}

		var lsn pages.LSN
		if err := read(src, &lsn); err != nil {
			return nil, src.TotalRead, err
		}

		lr := &LogRecord{T: TypeCommit, Lsn: lsn, TxnID: transaction.TxnID(t)}
		if LogRecordType(d.area[0]) == TypeCommit {
			freed, err := deSerUint64[uint64](src)
			if err != nil {
				return nil, src.TotalRead, err
			}
			lr.FreedPages = freed
		}

		return lr, src.TotalRead, nil
	}

	n, err = src.Read(d.area[1:])
	if err != nil {
		if err == io.EOF {
			return nil, src.TotalRead, ErrShortRead
		}
		return nil, src.TotalRead, err
	}

	if n != len(d.area[1:]) {
		return nil, src.TotalRead, ErrShortRead
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
		if err == io.EOF {
			return nil, src.TotalRead, ErrShortRead
		}
		return nil, src.TotalRead, err
	}
	if n != len(payload) {
		return nil, src.TotalRead, ErrShortRead
	}

	n, err = src.Read(oldpayload)
	if err != nil {
		if err == io.EOF {
			return nil, src.TotalRead, ErrShortRead
		}
		return nil, src.TotalRead, err
	}
	if n != len(oldpayload) {
		return nil, src.TotalRead, ErrShortRead
	}

	res.Payload = payload
	res.OldPayload = oldpayload

	return &res, src.TotalRead, nil
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

func read(r io.Reader, val any) error {
	if err := binary.Read(r, binary.BigEndian, val); err != nil {
		if err == io.ErrUnexpectedEOF {
			return ErrShortRead
		}

		return err
	}

	return nil
}

func serUint64[T uint64s](w LogWriter, arr []T, lsn pages.LSN) {
	s := [8]byte{}
	binary.BigEndian.PutUint64(s[:], uint64(len(arr)))
	n, err := w.Write(s[:], lsn)
	if err != nil {
		panic(err)
	}
	if n != len(s) {
		panic("short write")
	}

	for _, pageID := range arr {
		s := [8]byte{}
		binary.BigEndian.PutUint64(s[:], uint64(pageID))
		n, err := w.Write(s[:], lsn)
		if err != nil {
			panic(err)
		}
		if n != len(s) {
			panic("short write")
		}
	}
}

func deSerUint64[T uint64s](src io.Reader) ([]T, error) {
	var arrLen uint64
	if err := read(src, &arrLen); err != nil {
		return nil, err
	}

	arr := make([]T, 0)
	for i := uint64(0); i < arrLen; i++ {
		var t uint64
		if err := read(src, &t); err != nil {
			return nil, err
		}

		arr = append(arr, T(t))
	}

	return arr, nil
}

type uint64s interface {
	~uint64
}

//var _ LogRecordSerializer = &JsonLogRecordSerializer{}
//
//type JsonLogRecordSerializer struct {
//}
//
//func (j *JsonLogRecordSerializer) Serialize(r *LogRecord, writer LogWriter) {
//	if err := json.NewEncoder(writer).Encode(r); err != nil {
//		panic(err)
//	}
//}
//
//func (j *JsonLogRecordSerializer) Size(r *LogRecord) int {
//	//TODO implement me
//	panic("implement me")
//}
//
//func (j *JsonLogRecordSerializer) Deserialize(src io.Reader) (*LogRecord, int, error) {
//	record := LogRecord{}
//	if err := json.NewDecoder(src).Decode(&record); err != nil {
//		panic(err)
//	}
//
//	return &record, 0, nil // NOTE: TODO: returns 0
//}
