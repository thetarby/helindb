package btree

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

type KeySerializer interface {
	Serialize(key common.Key) ([]byte, error)
	Deserialize([]byte) (common.Key, error)
}

type ValueSerializer interface {
	Serialize(val interface{}) ([]byte, error)
	Deserialize([]byte) (interface{}, error)
}

type SlotPointer struct {
	PageId  int64
	SlotIdx int16
}

type SlotPointerValueSerializer struct{}

func (s *SlotPointerValueSerializer) Serialize(val interface{}) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, val)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *SlotPointerValueSerializer) Deserialize(data []byte) (interface{}, error) {
	reader := bytes.NewReader(data)
	var val SlotPointer
	err := binary.Read(reader, binary.BigEndian, &val)

	return val, err
}

type StringValueSerializer struct{}

func (s *StringValueSerializer) Serialize(val interface{}) ([]byte, error) {
	return []byte(val.(string)), nil
}

func (s *StringValueSerializer) Deserialize(data []byte) (interface{}, error) {
	return string(data[:]), nil
}

type StringKey string

func (p StringKey) String() string {
	return string(p)
}

func (p StringKey) Less(than common.Key) bool {
	return p < than.(StringKey)
}

type StringKeySerializer struct {
	Len int
}

func (s *StringKeySerializer) Serialize(key common.Key) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, ([]byte)(key.(StringKey)))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *StringKeySerializer) Deserialize(data []byte) (common.Key, error) {
	if s.Len < 0 {
		// then it is varchar
		return StringKey(data), nil
	}
	return StringKey(data[:s.Len]), nil
}

type PersistentKey int64

func (p PersistentKey) Less(than common.Key) bool {
	return p < than.(PersistentKey)
}

type PersistentKeySerializer struct{}

func (p *PersistentKeySerializer) Serialize(key common.Key) ([]byte, error) {
	b := make([]byte, 8, 8)
	binary.BigEndian.PutUint64(b, uint64(key.(PersistentKey)))

	return b, nil
}

func (p *PersistentKeySerializer) Deserialize(data []byte) (common.Key, error) {
	return PersistentKey(binary.BigEndian.Uint64(data)), nil
}
