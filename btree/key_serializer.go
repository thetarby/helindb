package btree

import (
	"bytes"
	"encoding/binary"
)

type KeySerializer interface {
	Serialize(key Key) ([]byte, error)
	Deserialize([]byte) (Key, error)
}

type PersistentKeySerializer struct{}

func (p *PersistentKeySerializer) Serialize(key Key) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, key.(PersistentKey))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (p *PersistentKeySerializer) Deserialize(data []byte) (Key, error) {
	reader := bytes.NewReader(data)
	var key PersistentKey
	err := binary.Read(reader, binary.BigEndian, &key)
	if err != nil {
		return nil, err
	}
	return key, nil
}

type StringKeySerializer struct {
	Len int
}

func (s *StringKeySerializer) Serialize(key Key) ([]byte, error) {
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, ([]byte)(key.(StringKey)))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (s *StringKeySerializer) Deserialize(data []byte) (Key, error) {
	return StringKey(data[:s.Len]), nil
}
