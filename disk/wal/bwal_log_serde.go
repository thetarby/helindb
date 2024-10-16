package wal

import (
	"encoding/binary"
	"encoding/json"
	"github.com/golang/snappy"
	"helin/common"
	"helin/disk/pages"
	"helin/transaction"
)

type JsonLogSerDe struct{}

var _ LogRecordSerDe = &JsonLogSerDe{}

func NewJsonLogSerDe() *JsonLogSerDe {
	return &JsonLogSerDe{}
}

func (l *JsonLogSerDe) Serialize(lr *LogRecord) []byte {
	b, err := json.Marshal(lr)
	if err != nil {
		panic(err)
	}

	return b
}

func (l *JsonLogSerDe) Deserialize(d []byte, lr *LogRecord) {
	if err := json.Unmarshal(d, lr); err != nil {
		panic(err)
	}

	lr.Raw = d
}

type BinarySerDe struct {
}

var _ LogRecordSerDe = &BinarySerDe{}

func NewBinarySerDe() *BinarySerDe {
	return &BinarySerDe{}
}

func (b *BinarySerDe) Serialize(lr *LogRecord) []byte {
	res := make([]byte, 0, 1000)
	res = append(res, byte(lr.T))
	res = binary.AppendUvarint(res, uint64(lr.TxnID))

	res = binary.AppendUvarint(res, uint64(len(lr.FreedPages)))
	for _, page := range lr.FreedPages {
		res = binary.AppendUvarint(res, page)
	}

	res = binary.AppendUvarint(res, uint64(lr.Lsn))
	res = binary.AppendUvarint(res, uint64(lr.PrevLsn))

	res = binary.AppendUvarint(res, uint64(lr.Idx))
	res = binary.AppendUvarint(res, uint64(len(lr.Payload)))
	res = append(res, lr.Payload...)

	res = binary.AppendUvarint(res, uint64(len(lr.OldPayload)))
	res = append(res, lr.OldPayload...)

	res = binary.AppendUvarint(res, lr.PageID)
	res = binary.AppendUvarint(res, lr.TailPageID)
	res = binary.AppendUvarint(res, lr.HeadPageID)

	res = binary.AppendUvarint(res, lr.PageType)

	res = append(res, common.Ternary(lr.IsClr, byte(1), byte(0)))

	res = binary.AppendUvarint(res, uint64(lr.UndoNext))

	res = binary.AppendUvarint(res, uint64(len(lr.Actives)))
	for _, t := range lr.Actives {
		res = binary.AppendUvarint(res, uint64(t))
	}

	return snappy.Encode(nil, res)
}

func (b *BinarySerDe) Deserialize(d []byte, lr *LogRecord) {
	data, err := snappy.Decode(nil, d)
	if err != nil {
		panic("corrupt log")
	}

	offset := 0
	uvarint := func() uint64 {
		res, n := binary.Uvarint(data[offset:])
		if n <= 0 {
			panic("corrupt log")
		}
		offset += n

		return res
	}

	lr.TxnID = transaction.TxnID(uvarint())

	// first read list length, then the items
	freedLen := uvarint()
	lr.FreedPages = make([]uint64, freedLen)
	for i := uint64(0); i < freedLen; i++ {
		lr.FreedPages[i] = uvarint()
	}

	lr.Lsn = pages.LSN(uvarint())
	lr.PrevLsn = pages.LSN(uvarint())
	lr.Idx = uint16(uvarint()) // TODO: panic when overflow

	// Read Payload length
	payloadLen := uvarint()
	lr.Payload = data[offset : offset+int(payloadLen)]
	offset += int(payloadLen)

	// Read OldPayload length
	oldPayloadLen := uvarint()
	lr.OldPayload = data[offset : offset+int(oldPayloadLen)]
	offset += int(oldPayloadLen)

	lr.PageID = uvarint()
	lr.TailPageID = uvarint()
	lr.HeadPageID = uvarint()
	lr.PageType = uvarint()

	lr.IsClr = data[offset] == 1
	offset++

	lr.UndoNext = pages.LSN(uvarint())

	// Read Actives length
	activesLen := uvarint()
	lr.Actives = make([]transaction.TxnID, activesLen)
	for i := uint64(0); i < activesLen; i++ {
		lr.Actives[i] = transaction.TxnID(uvarint())
	}
}

func NewDefaultSerDe() *BinarySerDe {
	return &BinarySerDe{}
}
