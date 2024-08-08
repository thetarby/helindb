package bwal

import (
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func openSegmentReader(dir string, segmentSize uint64) *SegmentReaderImpl {
	r, err := NewSegmentFS().OpenSegmentReader(dir, segmentSize)
	if err != nil {
		panic(err)
	}

	return r.(*SegmentReaderImpl)
}

func openSegmentWriter(dir string, segmentSize uint64) SegmentWriter {
	r, err := NewSegmentFS().OpenSegmentWriter(dir, segmentSize)
	if err != nil {
		panic(err)
	}

	return r
}

func TestSegmentReader_Read(t *testing.T) {
	t.Run("more than 1 segment long", func(t *testing.T) {
		var segmentSize, writtenBytes uint64 = 8192, 8192 + 100

		dir := makeTmpDir(t)
		sw := openSegmentWriter(dir, segmentSize)

		b := make([]byte, writtenBytes)
		b[0] = 255
		b[len(b)-1] = 255
		_, err := sw.Write(b, 0)
		assert.NoError(t, err)

		sr := openSegmentReader(dir, segmentSize)

		read := make([]byte, writtenBytes)
		_, err = sr.Read(read)
		assert.NoError(t, err)

		assert.Equal(t, b, read)
	})
}

func TestSegmentReaderImpl_Size(t *testing.T) {
	t.Run("size returns 0 when nothing is written", func(t *testing.T) {
		dir := makeTmpDir(t)

		sr := openSegmentReader(dir, 8192)
		s, err := sr.Size()
		assert.NoError(t, err)

		assert.Zero(t, s)
	})

	t.Run("size returns written bytes' size after first opened", func(t *testing.T) {
		dir := makeTmpDir(t)

		sw := openSegmentWriter(dir, 8192)

		size := 100
		n, err := sw.Write(make([]byte, size), 0)
		assert.NoError(t, err)
		assert.EqualValues(t, size, n)

		sr := openSegmentReader(dir, 8192)
		s, err := sr.Size()
		assert.NoError(t, err)

		assert.EqualValues(t, size, s)
	})

	t.Run("when there is more than one segment", func(t *testing.T) {
		t.Run("more than 1 full segments", func(t *testing.T) {
			dir := makeTmpDir(t)

			sw := openSegmentWriter(dir, 8192)

			size := 8192 * 2
			n, err := sw.Write(make([]byte, size), 0)
			assert.NoError(t, err)
			assert.EqualValues(t, size, n)

			sr := openSegmentReader(dir, 8192)
			s, err := sr.Size()
			assert.NoError(t, err)

			assert.EqualValues(t, size, s)
		})

		t.Run("more than 1 full segments and a partial segment", func(t *testing.T) {
			dir := makeTmpDir(t)

			sw := openSegmentWriter(dir, 8192)

			size := 8192*2 + 100
			n, err := sw.Write(make([]byte, size), 0)
			assert.NoError(t, err)
			assert.EqualValues(t, size, n)

			sr := openSegmentReader(dir, 8192)
			s, err := sr.Size()
			assert.NoError(t, err)

			assert.EqualValues(t, size, s)
		})
	})

	t.Run("truncate changes size", func(t *testing.T) {
		t.Run("when truncated mid-segment", func(t *testing.T) {
			dir := makeTmpDir(t)

			sw := openSegmentWriter(dir, 8192)

			size := 8192 + 1000
			truncateSize := uint64(8192 + 10)

			n, err := sw.Write(make([]byte, size), 0)
			assert.NoError(t, err)
			assert.EqualValues(t, size, n)

			sr := openSegmentReader(dir, 8192)
			assert.NoError(t, sr.Truncate(truncateSize))

			s, err := sr.Size()
			assert.NoError(t, err)

			assert.EqualValues(t, truncateSize, s)
		})

		t.Run("when truncated from segment end", func(t *testing.T) {
			dir := makeTmpDir(t)

			sw := openSegmentWriter(dir, 8192)

			size := 8192 + 1000
			truncateSize := uint64(8192)

			n, err := sw.Write(make([]byte, size), 0)
			assert.NoError(t, err)
			assert.EqualValues(t, size, n)

			sr := openSegmentReader(dir, 8192)
			assert.NoError(t, sr.Truncate(truncateSize))

			s, err := sr.Size()
			assert.NoError(t, err)

			assert.EqualValues(t, truncateSize, s)
		})
	})
}

func TestSegmentReaderImpl_Truncate(t *testing.T) {
	t.Run("mid-segment", func(t *testing.T) {
		dir := makeTmpDir(t)

		sw := openSegmentWriter(dir, 8192)

		size := 8192
		n, err := sw.Write(make([]byte, size), 0)
		assert.NoError(t, err)
		assert.EqualValues(t, size, n)

		sr := openSegmentReader(dir, 8192)
		assert.NoError(t, sr.Truncate(1000))

		res := make([]byte, 1001)
		n, err = sr.Read(res)
		assert.ErrorIs(t, err, io.EOF)
		assert.EqualValues(t, 1000, n)
	})

	t.Run("end of segment", func(t *testing.T) {
		dir := makeTmpDir(t)

		sw := openSegmentWriter(dir, 8192)

		size := 8192 * 2
		n, err := sw.Write(make([]byte, size), 0)
		assert.NoError(t, err)
		assert.EqualValues(t, size, n)

		sr := openSegmentReader(dir, 8192)
		assert.NoError(t, sr.Truncate(8192))

		res := make([]byte, 8193)
		n, err = sr.Read(res)
		assert.ErrorIs(t, err, io.EOF)
		assert.EqualValues(t, 8192, n)
	})

	t.Run("write after truncate", func(t *testing.T) {
		t.Run("mid-segment", func(t *testing.T) {
			dir := makeTmpDir(t)

			sw := openSegmentWriter(dir, 8192)

			size := 8192 * 2
			n, err := sw.Write(make([]byte, size), 0)
			assert.NoError(t, err)
			assert.EqualValues(t, size, n)

			truncSize := 8192 + 100
			sr := openSegmentReader(dir, 8192)
			assert.NoError(t, sr.Truncate(uint64(truncSize)))

			toWrite := []byte("new after truncate")
			_, err = sw.Write(toWrite, 0)
			assert.NoError(t, err)

			res := make([]byte, truncSize+len(toWrite))
			n, err = sr.Read(res)
			assert.NoError(t, err)

			for _, v := range res[:truncSize] {
				assert.Zero(t, v)
			}

			assert.Equal(t, toWrite, res[truncSize:])
		})
	})
}

func TestSegmentReaderImpl_TruncateFront(t *testing.T) {
	t.Run("mid-segment", func(t *testing.T) {
		dir := makeTmpDir(t)
		sw := openSegmentWriter(dir, 8192)

		truncateSize, size := uint64(1000), 8192
		n, err := sw.Write(make([]byte, size), 0)
		assert.NoError(t, err)
		assert.EqualValues(t, size, n)

		sr := openSegmentReader(dir, 8192)
		assert.NoError(t, sr.TruncateFront(truncateSize))

		remainingSize := size - int(truncateSize)
		res := make([]byte, remainingSize+1)
		n, err = sr.Read(res)
		assert.ErrorIs(t, err, io.EOF)
		assert.EqualValues(t, remainingSize, n)
	})

	t.Run("end of segment", func(t *testing.T) {
		dir := makeTmpDir(t)
		sw := openSegmentWriter(dir, 8192)

		size := 8192 * 2
		n, err := sw.Write(make([]byte, size), 0)
		assert.NoError(t, err)
		assert.EqualValues(t, size, n)

		sr := openSegmentReader(dir, 8192)
		assert.NoError(t, sr.TruncateFront(8192))
		assert.NoError(t, sr.Close())

		sr = openSegmentReader(dir, 8192)
		res := make([]byte, 8193)
		n, err = sr.Read(res)
		assert.ErrorIs(t, err, io.EOF)
		assert.EqualValues(t, 8192, n)
	})

	t.Run("write after truncate", func(t *testing.T) {
		t.Run("mid-segment", func(t *testing.T) {
			dir := makeTmpDir(t)
			sw := openSegmentWriter(dir, 8192)

			size := 8192 * 2
			n, err := sw.Write(make([]byte, size), 0)
			assert.NoError(t, err)
			assert.EqualValues(t, size, n)

			truncSize := 8192 + 100
			sr := openSegmentReader(dir, 8192)
			assert.NoError(t, sr.TruncateFront(uint64(truncSize)))
			assert.NoError(t, sr.Close())

			toWrite := []byte("new after truncate")
			_, err = sw.Write(toWrite, 0)
			assert.NoError(t, err)

			sr = openSegmentReader(dir, 8192)
			res := make([]byte, size-truncSize+len(toWrite))
			n, err = sr.Read(res)
			assert.NoError(t, err)

			for _, v := range res[:size-truncSize] {
				assert.Zero(t, v)
			}

			assert.Equal(t, toWrite, res[size-truncSize:])
		})
	})
}

func TestSegmentFSImpl_OpenSegmentReader(t *testing.T) {
	t.Run("when there is an uninitialized segment", func(t *testing.T) {
		dir := makeTmpDir(t)

		sw := openSegmentWriter(dir, 8192)
		_, err := sw.Write(make([]byte, 8192), 0)
		assert.NoError(t, err)

		sr := openSegmentReader(dir, 8192)
		origSeg, err := sr.LastSegment()
		assert.NoError(t, err)
		assert.NoError(t, sr.Close())

		unInitSegmentPath := filepath.Join(dir, segmentToStr(origSeg+1))
		f, err := os.OpenFile(unInitSegmentPath, os.O_RDWR|os.O_CREATE, DefaultOptions.FilePerms)
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		sr = openSegmentReader(dir, 8192)
		seg, err := sr.LastSegment()
		assert.NoError(t, err)
		assert.EqualValues(t, origSeg, seg)
	})
}

func TestSegmentFSImpl_OpenSegmentWriter(t *testing.T) {
	t.Run("when there is an uninitialized segment", func(t *testing.T) {
		dir := makeTmpDir(t)

		sw := openSegmentWriter(dir, 8192)
		_, err := sw.Write(make([]byte, 8192), 0)
		assert.NoError(t, err)

		sr := openSegmentReader(dir, 8192)
		origSeg, err := sr.LastSegment()
		assert.NoError(t, err)
		assert.NoError(t, sr.Close())

		unInitSegmentPath := filepath.Join(dir, segmentToStr(origSeg+1))
		f, err := os.OpenFile(unInitSegmentPath, os.O_RDWR|os.O_CREATE, DefaultOptions.FilePerms)
		assert.NoError(t, err)
		assert.NoError(t, f.Close())

		sw = openSegmentWriter(dir, 8192)
		_, err = sw.Write(make([]byte, 8192), 0)
		assert.NoError(t, err)
		assert.NoError(t, sw.Close())

		sr = openSegmentReader(dir, 8192)
		assert.NoError(t, err)

		si, err := sr.Size()
		assert.NoError(t, err)
		assert.EqualValues(t, 8192*2, si)

		lastSeg, err := sr.LastSegment()
		assert.NoError(t, err)
		assert.EqualValues(t, origSeg+1, lastSeg)
	})
}
