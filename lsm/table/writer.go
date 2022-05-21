package sstable

import (
	"encoding/binary"
	"fmt"
	"helin/lsm/common"
	"io"

	"github.com/pkg/errors"
)

// TODO: what should be its size?
const footerLen = 48
const magic = "\x57\xfb\x80\x8b\x24\x75\x47\xdb"

type Writer struct {
	writer        io.Writer
	offset        uint64
	err           error
	cmp           Comparer
	prevKey       []byte // holds last key which is written
	restartOffets []uint32
	buf           []byte
	nEntries      int

	restartInterval int
	blockSize       int

	// A table is a series of blocks and a block's index entry contains a
	// separator key between one block and the next. Thus, a finished block
	// cannot be written until the first key in the next block is seen.
	// pendingBH is the blockHandle of a finished block that is waiting for
	// the next call to Set. If the writer is not in this state, pendingBH
	// is zero.
	pendingBH blockHandle

	indexEntries []indexEntry
}

func (w *Writer) Set(key, value []byte) error {
	if w.cmp.Compare(w.prevKey, key) >= 0 {
		w.err = fmt.Errorf("leveldb/table: Set called in non-increasing key order: %q, %q", w.prevKey, key)
		return w.err
	}

	w.flushPendingBH(key)
	w.writeEntry(key, value, w.nEntries%w.restartInterval == 0)
	// If the estimated block size is sufficiently large, finish the current block.
	if len(w.buf)+4*(len(w.restartOffets)+1) >= w.blockSize {
		bh, err := w.finishBlock()
		if err != nil {
			w.err = err
			return w.err
		}
		w.pendingBH = bh
	}

	return nil
}

func (w *Writer) Close() (err error) {
	if w.err != nil {
		return w.err
	}

	// Finish the last data block, or force an empty data block if there
	// aren't any data blocks at all.
	w.flushPendingBH(nil)
	if w.nEntries > 0 || len(w.indexEntries) == 0 {
		bh, err := w.finishBlock()
		if err != nil {
			w.err = err
			return w.err
		}
		w.pendingBH = bh
		w.flushPendingBH(nil)
	}

	// TODO: what should be tmpBuf size?
	tempBuf := make([]byte, 50)

	// Write the index block.
	for _, ie := range w.indexEntries {
		n := encodeBlockHandle(tempBuf, ie.bh)
		w.writeEntry(ie.key, tempBuf[:n], true)
	}
	indexBlockHandle, err := w.finishBlock()
	if err != nil {
		w.err = err
		return w.err
	}

	// Write the table footer.
	footer := tempBuf[:footerLen]
	for i := range footer {
		footer[i] = 0
	}

	encodeBlockHandle(footer, indexBlockHandle)
	copy(footer[footerLen-len(magic):], magic)
	if _, err := w.writer.Write(footer); err != nil {
		w.err = err
		return w.err
	}

	// Make any future calls to Set or Close return an error.
	w.err = errors.New("leveldb/table: writer is closed")
	return nil
}

// flushPendingBH adds pendingBH to index block but it is done on memory. index block will be written to io.Writer
// when Close is called.
func (w *Writer) flushPendingBH(key []byte) {
	if w.pendingBH.length == 0 {
		// A valid blockHandle must be non-zero.
		// In particular, it must have a non-zero length.
		return
	}
	sep := w.cmp.Separator(w.prevKey, key)
	w.indexEntries = append(w.indexEntries, indexEntry{w.pendingBH, sep})
	w.pendingBH = blockHandle{}
}

// writeEntry appends a key/value pair, which may also be a restart point.
func (w *Writer) writeEntry(key, value []byte, restart bool) {
	nShared := 0
	if restart {
		w.restartOffets = append(w.restartOffets, uint32(len(w.buf)))
	} else {
		nShared = common.SharedPrefixLen(w.prevKey, key)
	}

	w.prevKey = append(w.prevKey[:0], key...)
	w.nEntries++
	buf := make([]byte, 24) // at most 3 bytes
	n := binary.PutUvarint(buf[0:], uint64(nShared))
	n += binary.PutUvarint(buf[n:], uint64(len(key)-nShared))
	n += binary.PutUvarint(buf[n:], uint64(len(value)))
	w.buf = append(w.buf, buf[:n]...)
	w.buf = append(w.buf, key[nShared:]...)
	w.buf = append(w.buf, value...)
}

// finishBlock finishes the current block and returns its block handle, which is
// its offset and length in the table.
func (w *Writer) finishBlock() (blockHandle, error) {
	// Write the restart points to the buffer.
	if w.nEntries == 0 {
		// Every block must have at least one restart point.
		w.restartOffets = w.restartOffets[:1]
		w.restartOffets[0] = 0
	}

	// encode restartOffsets to the end of buffer
	tmp4 := make([]byte, 4)
	for _, x := range w.restartOffets {
		binary.LittleEndian.PutUint32(tmp4, x)
		w.buf = append(w.buf, tmp4...)
	}

	// write NumberOfRestartPoints to the end of buffer
	binary.LittleEndian.PutUint32(tmp4, uint32(len(w.restartOffets)))
	w.buf = append(w.buf, tmp4...)

	bh, err := w.writeRawBlock(w.buf)

	// Reset the per-block state.
	w.buf = w.buf[:0]
	w.nEntries = 0
	w.restartOffets = w.restartOffets[:0]

	return bh, err
}

func (w *Writer) writeRawBlock(b []byte) (blockHandle, error) {
	// Write the bytes to the file.
	if _, err := w.writer.Write(b); err != nil {
		return blockHandle{}, err
	}

	bh := blockHandle{w.offset, uint64(len(b))}
	w.offset += uint64(len(b))
	return bh, nil
}

func NewWriter(ioWriter io.Writer, cmp Comparer) *Writer {
	writer := &Writer{
		writer:          ioWriter,
		offset:          0,
		err:             nil,
		cmp:             cmp,
		prevKey:         []byte{},
		restartOffets:   []uint32{},
		buf:             []byte{},
		nEntries:        0,
		restartInterval: 16,
		blockSize:       4096,
		pendingBH:       blockHandle{},
	}

	return writer
}

// indexEntry's value is a block handle and key is a separator between blocks.
type indexEntry struct {
	bh     blockHandle
	key    []byte
}
