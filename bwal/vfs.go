package bwal

import (
	"io"
	"os"
)

type FileInfo struct {
	Size int64
}

type FS interface {
	Open(name string) (BlockFile, error)
	OpenFile(name string, flag int, perm os.FileMode) (BlockFile, error)
	Remove(name string) error
	ReadDir(name string) (names []string, err error)
	Stat(name string) (FileInfo, error)
}

type BlockFile interface {
	io.ReadWriteSeeker
	io.Closer
	Truncate(size int64) error
}
