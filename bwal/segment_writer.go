package bwal

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
)

const SegmentHeaderSize = 16

type SegmentHeader struct {
	LastLSN uint64
	StartAt uint64
}

func writeSegmentHeader(h SegmentHeader, dest []byte) {
	binary.BigEndian.PutUint64(dest, h.LastLSN)
	binary.BigEndian.PutUint64(dest[8:], h.StartAt)
}

func readSegmentHeader(src []byte) SegmentHeader {
	prevLSN := binary.BigEndian.Uint64(src)
	startAt := binary.BigEndian.Uint64(src[8:])

	return SegmentHeader{
		LastLSN: prevLSN,
		StartAt: startAt,
	}
}

type SegmentWriter interface {
	Write(block []byte, lastLsn uint64) (int, error)
	io.Closer
}

type SegmentWriterImpl struct {
	path        string
	sfile       BlockFile
	fs          FS
	currSegment *uint64
	lastLSN     uint64
	n           int
	segmentSize uint64
}

func NewSegmentWriter(path string, startLSN uint64, segmentSize uint64) *SegmentWriterImpl {
	w := &SegmentWriterImpl{path: path, segmentSize: segmentSize, n: 0, lastLSN: startLSN, fs: newFsV1()}

	if err := w.Cycle(); err != nil {
		panic(err)
	}

	return w
}

func (w *SegmentWriterImpl) Cycle() error {
	if w.sfile != nil {
		if err := w.sfile.Close(); err != nil {
			return err
		}
	}

	seg := uint64(0)
	if w.currSegment != nil {
		seg = *w.currSegment + 1
	}

	path := filepath.Join(w.path, segmentToStr(seg))

	var err error
	w.sfile, err = w.fs.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, DefaultOptions.FilePerms)
	if err != nil {
		return err
	}

	w.currSegment = &seg

	// write segment header first
	segmentHeaderb := [SegmentHeaderSize]byte{}
	writeSegmentHeader(SegmentHeader{
		LastLSN: w.lastLSN,
	}, segmentHeaderb[:])

	_, err = w.sfile.Write(segmentHeaderb[:])
	if err != nil {
		return err
	}

	w.n = 0
	return err
}

func (w *SegmentWriterImpl) Overflow() bool {
	return w.n > int(w.segmentSize)
}

func (w *SegmentWriterImpl) Offset() uint64 {
	return (w.segmentSize * (*w.currSegment)) + uint64(w.n)
}

// block might be one log record or a batch of log records. lastLsn is the lsn of the last log record in the data.
func (w *SegmentWriterImpl) Write(block []byte, lastLsn uint64) (int, error) {
	nTotal := 0
	for {
		end := min(nTotal+(int(w.segmentSize)-w.n), len(block))

		// TODO: might happen zero writes if nTotal == end
		n, err := w.sfile.Write(block[nTotal:end])
		if err != nil {
			return 0, err
		}

		nTotal += n
		w.n += n

		if nTotal == len(block) {
			break
		} else if nTotal > len(block) {
			// assert this should not happen
			return 0, errors.New("corrupt")
		}

		if err := w.Cycle(); err != nil {
			return 0, err
		}
	}

	w.lastLSN = lastLsn

	return nTotal, nil
}

func (w *SegmentWriterImpl) Close() error {
	if w.sfile != nil {
		return w.sfile.Close()
	}

	return nil
}
