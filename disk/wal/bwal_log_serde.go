package wal

import (
	"encoding/binary"
	"encoding/json"
	"github.com/golang/snappy"
	"helin/common"
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

	//if len(res) > 500 {
	//	return snappy.Encode(nil, res)
	//}

	return snappy.Encode(nil, res)
}

func (b *BinarySerDe) Deserialize(d []byte, lr *LogRecord) {

}
