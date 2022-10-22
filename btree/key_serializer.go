package btree

import (
	"bytes"
	"encoding/binary"
	"helin/common"
)

type KeySerializer interface {
	Serialize(key common.Key) ([]byte, error)
	Deserialize([]byte) (common.Key, error)
	Size() int
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

func (p *PersistentKeySerializer) Size() int {
	return 10
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

func (s *StringKeySerializer) Size() int {
	return s.Len
}

type ValueSerializer interface {
	Serialize(val interface{}) ([]byte, error)
	Deserialize([]byte) (interface{}, error)
	Size() int
}

type StringValueSerializer struct {
	Len int
}

func (s *StringValueSerializer) Serialize(val interface{}) ([]byte, error) {
	if s.Len < 0 {
		return []byte(val.(string)), nil
	}

	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, ([]byte)(val.(string)))
	if err != nil {
		return nil, err
	}
	res := make([]byte, s.Len)
	copy(res, buf.Bytes())
	return res, nil
}

func (s *StringValueSerializer) Deserialize(data []byte) (interface{}, error) {
	if s.Len < 0 {
		return string(data[:]), nil
	}
	return string(data[:s.Len]), nil
}

func (s *StringValueSerializer) Size() int {
	return s.Len
}

type SlotPointerValueSerializer struct {
}

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

func (s *SlotPointerValueSerializer) Size() int {
	return 10
}
