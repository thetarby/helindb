package sstable

import (
	"bytes"
	"io"
	"os"

	"github.com/pkg/errors"
)

var NotFound error = errors.New("key not found")

type Reader struct {
	file            io.ReaderAt
	err             error
	index           block
	comparer        Comparer
}

func (r *Reader) Get(key []byte) (value []byte, err error) {
	if r.err != nil {
		return nil, r.err
	}

	tableIt := NewTableIter(key, r.file, r.index, r.comparer)
	if !tableIt.Next() || !bytes.Equal(key, tableIt.Key()) {
		err := tableIt.Close()
		if err == nil {
			return nil, NotFound
		}
		return nil, err
	}

	return tableIt.Value(), tableIt.Close()
}

type File interface {
	io.Closer
	io.Reader
	io.ReaderAt
	io.Writer
	Stat() (os.FileInfo, error)
	Sync() error
}

func NewReader(file File, cmp Comparer) (*Reader, error){
	s, err := file.Stat()
	if err != nil {
		return nil, errors.Wrap(err, "error while reading file stat")
	}

	var footer [footerLen]byte
	_, err = file.ReadAt(footer[:], s.Size()-int64(len(footer)))
	if err != nil && err != io.EOF {
		return nil, errors.Wrap(err, "error while reading table footer")
	}

	if string(footer[footerLen-len(magic):footerLen]) != magic {
		return nil, errors.New("invalid file, bad magic number")
	}

	idxBH, _ := decodeBlockHandle(footer[:])
	idxBlock := make([]byte, idxBH.length)
	file.ReadAt(idxBlock, int64(idxBH.offset))

	return &Reader{
		file:     file,
		err:      nil,
		index:    idxBlock,
		comparer: cmp,
	}, nil
}