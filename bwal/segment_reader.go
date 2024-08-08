package bwal

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
)

// TODO: close files

var ErrUninitializedSegmentFile = errors.New("segment file is not initialized")
var ErrOutOfBounds = errors.New("tried to seek out of file bounds")
var ErrNoSegmentFile = errors.New("there is no segment files")

var _ io.ReadSeeker = &SegmentReaderImpl{}

type SegmentReader interface {
	io.ReadSeekCloser
	Truncate(size uint64) error
	TruncateFront(size uint64) error
	LastSegmentHeader() (*SegmentHeader, error)
	FirstSegmentHeader() (*SegmentHeader, error)
	Size() (uint64, error)
	FrontTruncatedSize() (uint64, error)
}

type SegmentReaderImpl struct {
	dir               string
	segmentSize       uint64
	currSegment       *uint64
	currSegmentHeader *SegmentHeader
	currSegmentFile   BlockFile
	fs                FS
}

func (r *SegmentReaderImpl) Seek(offset int64, whence int) (int64, error) {
	seg := lsnToSegment(uint64(offset), r.segmentSize)

	if r.currSegment == nil || *r.currSegment != seg {
		_, err := r.openAsCurrentSegment(seg)
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return 0, ErrOutOfBounds
			}

			return 0, err
		}
	}

	segmentOffset := lsnToSegmentOffset(uint64(offset), r.segmentSize)
	if segmentOffset < r.currSegmentHeader.StartAt {
		return 0, ErrOutOfBounds
	}

	if _, err := r.currSegmentFile.Seek(int64(segmentOffset)+SegmentHeaderSize, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to seek segment:%v ,error: %w", seg, err)
	}

	return 0, nil
}

func (r *SegmentReaderImpl) Read(dest []byte) (int, error) {
	if r.currSegmentFile == nil {
		if err := r.nextSegment(); err != nil {
			return 0, err
		}
	}

	nTotal := 0
	for {
		n, err := r.currSegmentFile.Read(dest[nTotal:])
		if err != nil && !errors.Is(err, io.EOF) {
			return n, err
		}

		nTotal += n

		if nTotal == len(dest) {
			break
		} else if nTotal > len(dest) {
			// assert this should not happen
			return nTotal, errors.New("corrupt")
		}

		if err := r.nextSegment(); err != nil {
			return nTotal, err
		}
	}

	return nTotal, nil
}

func (r *SegmentReaderImpl) Truncate(size uint64) error {
	seg := lsnToSegment(size, r.segmentSize)

	// first delete all segments until segment to truncate starting from the last one
	currSeg, err := r.LastSegment()
	if err != nil {
		return err
	}

	for {
		if currSeg == seg {
			break
		}

		if err := r.delSegment(seg); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return fmt.Errorf("failed to delete segment: %v, err: %w", seg, err)
		}
		currSeg--
	}

	// then truncate the segment that size splits.
	_, f, err := r.openSegment(seg)
	if err != nil {
		return fmt.Errorf("failed to open segment:%v ,error: %w", seg, err)
	}

	offset := lsnToSegmentOffset(size, r.segmentSize)
	if err := f.Truncate(int64(offset + SegmentHeaderSize)); err != nil {
		return fmt.Errorf("failed to truncate segment: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close segment: %v, err: %w", seg, err)
	}

	return nil
}

func (r *SegmentReaderImpl) TruncateFront(size uint64) error {
	seg := lsnToSegment(size, r.segmentSize)

	segOffset := lsnToSegmentOffset(size, r.segmentSize)

	// first delete all the segments before that starting from first
	currSeg, err := r.FirstSegment()
	if err != nil {
		return err
	}

	for {
		if currSeg == seg {
			break
		}

		if err := r.delSegment(currSeg); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return fmt.Errorf("failed to delete segment: %v, err: %w", seg, err)
		}
		currSeg++
	}

	// if truncating from the end of the segment
	if segOffset == r.segmentSize-1 {
		if err := r.delSegment(seg); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}

			return fmt.Errorf("failed to delete segment: %v, err: %w", seg, err)
		}
	} else {
		segHeader, f, err := r.openSegment(seg)
		if err != nil {
			return err
		}

		segHeader.StartAt = segOffset
		segmentHeaderB := [SegmentHeaderSize]byte{}
		writeSegmentHeader(*segHeader, segmentHeaderB[:])

		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}

		if _, err := f.Write(segmentHeaderB[:]); err != nil {
			return err
		}
	}

	return nil
}

func (r *SegmentReaderImpl) LastSegment() (uint64, error) {
	entries, err := r.fs.ReadDir(r.dir)
	if err != nil {
		return 0, err
	}

	found := false
	maxSegment := uint64(0)
	for _, entry := range entries {
		s, err := segmentNameToSegment(entry)
		if err == nil {
			found = true
			maxSegment = max(maxSegment, s)
		}
	}

	if !found {
		return 0, ErrNoSegmentFile
	}

	return maxSegment, nil
}

func (r *SegmentReaderImpl) LastSegmentHeader() (*SegmentHeader, error) {
	entries, err := r.fs.ReadDir(r.dir)
	if err != nil {
		return nil, err
	}

	found := false
	maxSegment := uint64(0)
	for _, entry := range entries {
		s, err := segmentNameToSegment(entry)
		if err == nil {
			found = true
			maxSegment = max(maxSegment, s)
		}
	}

	if !found {
		return nil, ErrNoSegmentFile
	}

	return r.segmentHeader(maxSegment)
}

func (r *SegmentReaderImpl) FirstSegment() (uint64, error) {
	entries, err := r.fs.ReadDir(r.dir)
	if err != nil {
		return 0, err
	}

	var minSegment uint64 = math.MaxUint64
	for _, entry := range entries {
		s, err := segmentNameToSegment(entry)
		if err == nil {
			minSegment = min(minSegment, s)
		}
	}

	return minSegment, nil
}

func (r *SegmentReaderImpl) FirstSegmentHeader() (*SegmentHeader, error) {
	seg, err := r.FirstSegment()
	if err != nil {
		return nil, err
	}

	return r.segmentHeader(seg)
}

func (r *SegmentReaderImpl) Size() (uint64, error) {
	entries, err := r.fs.ReadDir(r.dir)
	if err != nil {
		return 0, err
	}

	segFound := false
	maxSegment := uint64(0)
	for _, entry := range entries {
		s, err := segmentNameToSegment(entry)
		if err == nil {
			segFound = true
			maxSegment = max(maxSegment, s)
		}
	}

	if !segFound {
		return 0, nil
	}

	stat, err := r.segmentStat(maxSegment)
	if err != nil {
		return 0, err
	}

	lastSegmentSize := stat.Size - SegmentHeaderSize
	otherSegments := maxSegment * r.segmentSize

	return uint64(lastSegmentSize) + otherSegments, nil
}

func (r *SegmentReaderImpl) FrontTruncatedSize() (uint64, error) {
	first, err := r.FirstSegment()
	if err != nil {
		return 0, err
	}

	h, err := r.FirstSegmentHeader()
	if err != nil {
		return 0, err
	}

	return first*r.segmentSize + h.StartAt, nil
}

func (r *SegmentReaderImpl) Close() error {
	if r.currSegmentFile != nil {
		return r.currSegmentFile.Close()
	}

	return nil
}

func (r *SegmentReaderImpl) delSegment(segment uint64) error {
	fileName := filepath.Join(r.dir, segmentToStr(segment))
	return r.fs.Remove(fileName)
}

// openSegment opens segment file and seeks cursor to the start of the payload
func (r *SegmentReaderImpl) openSegment(segment uint64) (*SegmentHeader, BlockFile, error) {
	f, err := r.fs.OpenFile(filepath.Join(r.dir, segmentToStr(segment)), os.O_RDWR, DefaultOptions.FilePerms)
	if err != nil {
		return nil, nil, err
	}

	// TODO: parse header and validate things
	headerB := [SegmentHeaderSize]byte{}
	n, err := f.Read(headerB[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, nil, ErrUninitializedSegmentFile
		}

		return nil, nil, err
	}
	if n != SegmentHeaderSize {
		if n == 0 {
			return nil, nil, ErrUninitializedSegmentFile
		}

		return nil, nil, errors.New("corrupt")
	}

	header := readSegmentHeader(headerB[:])
	if _, err := f.Seek(SegmentHeaderSize+int64(header.StartAt), io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("failed to seek segment:%v ,error: %w", segment, err)
	}

	return &header, f, nil
}

// openAsCurrentSegment closes current segment file if there is any, and opens segment and sets it as current segment
func (r *SegmentReaderImpl) openAsCurrentSegment(segment uint64) (BlockFile, error) {
	if err := r.closeCurrentSegment(); err != nil {
		return nil, fmt.Errorf("failed to close current segment: %w", err)
	}

	r.currSegment = nil
	r.currSegmentFile = nil
	r.currSegmentHeader = nil

	h, f, err := r.openSegment(segment)
	if err != nil {
		return nil, fmt.Errorf("failed to open segment:%v ,error: %w", segment, err)
	}

	r.currSegment = &segment
	r.currSegmentFile = f
	r.currSegmentHeader = h

	return f, nil
}

// segmentHeader opens segment file, reads header, closes the file and returns header
func (r *SegmentReaderImpl) segmentHeader(segment uint64) (*SegmentHeader, error) {
	f, err := r.fs.Open(filepath.Join(r.dir, segmentToStr(segment)))
	if err != nil {
		return nil, err
	}

	headerB := [SegmentHeaderSize]byte{}
	n, err := f.Read(headerB[:])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			return nil, ErrUninitializedSegmentFile
		}

		return nil, err
	}
	if n != SegmentHeaderSize {
		if n == 0 {
			return nil, ErrUninitializedSegmentFile
		}

		return nil, errors.New("corrupt")
	}

	header := readSegmentHeader(headerB[:])

	if err := f.Close(); err != nil {
		return nil, err
	}

	return &header, nil
}

// nextSegment calls openAsCurrentSegment for the next segment
func (r *SegmentReaderImpl) nextSegment() error {
	var seg uint64
	if r.currSegment != nil {
		seg = *r.currSegment + 1
	} else {
		var err error
		seg, err = r.FirstSegment()
		if err != nil {
			return err
		}
	}

	_, err := r.openAsCurrentSegment(seg)
	if err != nil && errors.Is(err, fs.ErrNotExist) {
		return io.EOF
	}

	return err
}

func (r *SegmentReaderImpl) closeCurrentSegment() error {
	if r.currSegmentFile != nil {
		return r.currSegmentFile.Close()
	}

	return nil
}

func (r *SegmentReaderImpl) segmentStat(segment uint64) (FileInfo, error) {
	f, err := r.fs.Stat(filepath.Join(r.dir, segmentToStr(segment)))
	if err != nil {
		return f, err
	}

	return f, nil
}
