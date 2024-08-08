package wal

import (
	"encoding/json"
)

type JsonLogSerDe struct{}

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
}

var _ LogRecordSerDe = &JsonLogSerDe{}
