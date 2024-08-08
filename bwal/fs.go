package bwal

import "os"

var _ BlockFile = &file{}

type file struct {
	fi os.File
}

func openAsBlockFile(name string, flag int, perm os.FileMode) (*file, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}

	return &file{
		fi: *f,
	}, nil
}

func openAsBlockFileToRead(name string) (*file, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	return &file{
		fi: *f,
	}, nil
}

func (f *file) Read(p []byte) (n int, err error) {
	return f.fi.Read(p)
}

func (f *file) Write(p []byte) (n int, err error) {
	n, err = f.fi.Write(p)
	if err != nil {
		return n, err
	}

	err = f.fi.Sync()
	return n, err
}

func (f *file) Seek(offset int64, whence int) (int64, error) {
	return f.fi.Seek(offset, whence)
}

func (f *file) Close() error {
	if err := f.fi.Sync(); err != nil {
		return err
	}

	return f.fi.Close()
}

func (f *file) Truncate(size int64) error {
	return f.fi.Truncate(size)
}

type fsV1 struct{}

func newFsV1() *fsV1 {
	return &fsV1{}
}

var _ FS = &fsV1{}

func (f fsV1) Open(name string) (BlockFile, error) {
	return openAsBlockFileToRead(name)
}

func (f fsV1) OpenFile(name string, flag int, perm os.FileMode) (BlockFile, error) {
	return openAsBlockFile(name, flag, perm)
}

func (f fsV1) Remove(name string) error {
	return os.Remove(name)
}

func (f fsV1) ReadDir(name string) (names []string, err error) {
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, err
	}

	names = make([]string, len(entries))
	for i, entry := range entries {
		names[i] = entry.Name()
	}

	return names, nil
}

func (f fsV1) Stat(name string) (FileInfo, error) {
	i, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, err
	}

	return FileInfo{
		Size: i.Size(),
	}, nil
}
