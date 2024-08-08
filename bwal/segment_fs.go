package bwal

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type SegmentFS interface {
	OpenSegmentWriter(dir string, segmentSize uint64) (SegmentWriter, error)
	OpenSegmentReader(dir string, segmentSize uint64) (SegmentReader, error)
}

type SegmentFSImpl struct {
	fs FS
}

var _ SegmentFS = &SegmentFSImpl{}

func NewSegmentFS() *SegmentFSImpl {
	return &SegmentFSImpl{
		fs: newFsV1(),
	}
}

func (s *SegmentFSImpl) OpenSegmentWriter(dir string, segmentSize uint64) (SegmentWriter, error) {
	if err := s.repair(dir); err != nil {
		return nil, fmt.Errorf("failed to repair: %w", err)
	}

	segFound, maxSegment, err := s.lastSegment(dir)
	if err != nil {
		return nil, err
	}

	if !segFound {
		// if there is no segment file
		w := &SegmentWriterImpl{
			path:        dir,
			sfile:       nil,
			fs:          newFsV1(),
			currSegment: nil,
			lastLSN:     0,
			n:           0,
			segmentSize: segmentSize,
		}

		if err := w.Cycle(); err != nil {
			panic(err)
		}

		return w, nil
	} else {
		seg := maxSegment

		// if there is already a segment file
		path := filepath.Join(dir, segmentToStr(maxSegment))

		sfile, err := s.fs.OpenFile(path, os.O_RDWR, DefaultOptions.FilePerms)
		if err != nil {
			return nil, err
		}

		i, err := s.fs.Stat(path)
		if err != nil {
			return nil, err
		}

		_, err = sfile.Seek(i.Size, io.SeekStart)
		if err != nil {
			return nil, err
		}

		w := &SegmentWriterImpl{
			path:        dir,
			sfile:       sfile,
			fs:          newFsV1(),
			currSegment: &seg,
			lastLSN:     0,
			n:           int(i.Size - SegmentHeaderSize),
			segmentSize: segmentSize,
		}

		return w, nil
	}
}

func (s *SegmentFSImpl) OpenSegmentReader(dir string, segmentSize uint64) (SegmentReader, error) {
	if err := s.repair(dir); err != nil {
		return nil, fmt.Errorf("failed to repair: %w", err)
	}

	return &SegmentReaderImpl{
		dir:         dir,
		segmentSize: segmentSize,
		fs:          newFsV1(),
	}, nil
}

// repair deletes last segment if it is not initialized(empty file with no header). It might happen cycling segments
// when file is created but a crash occurs before writing its header.
func (s *SegmentFSImpl) repair(dir string) error {
	found, lastSeg, err := s.lastSegment(dir)
	if err != nil {
		return err
	}

	if !found {
		return nil
	}

	path := filepath.Join(dir, segmentToStr(lastSeg))
	i, err := s.fs.Stat(path)
	if err != nil {
		return err
	}

	// TODO: test edge cases (opening initialized empty segments)
	if i.Size >= SegmentHeaderSize {
		return nil
	}

	return os.Remove(path)
}

func (s *SegmentFSImpl) lastSegment(dir string) (bool, uint64, error) {
	entries, err := s.fs.ReadDir(dir)
	if err != nil {
		return false, 0, err
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

	return segFound, maxSegment, nil
}
