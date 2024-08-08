package bwal

import (
	"encoding/binary"
	"errors"
	"log"
	"sync"
	"time"
)

var ErrWriterClosed = errors.New("writer is closed")

var LogRecordHeaderSize = binary.Size(LogRecordHeader{})

type LogRecordHeader struct {
	Size    uint16
	PrevLSN uint64
}

func writeLogRecordHeader(h LogRecordHeader, dest []byte) {
	binary.BigEndian.PutUint16(dest, h.Size)
	binary.BigEndian.PutUint64(dest[2:], h.PrevLSN)
}

func readLogRecordHeader(src []byte) LogRecordHeader {
	size := binary.BigEndian.Uint16(src)
	prevLSN := binary.BigEndian.Uint64(src[2:])

	return LogRecordHeader{
		Size:    size,
		PrevLSN: prevLSN,
	}
}

type BufferedLogWriter struct {
	buf          []byte
	totalWritten uint64
	offset       int
	latestInBuf  uint64
	bufComplete  bool

	flushBuf         []byte
	flushOffset      int
	latestInFlushBuf uint64
	flushBufComplete bool

	latestFlushed *uint64

	// underlying writer
	w SegmentWriter

	// swapMut is to synchronize swap operation. there can only be one swap operation going on at any time.
	swapMut *sync.Mutex

	// bufMut is to protect buffer from swapping in the middle of writing process.
	bufMut *sync.Mutex

	// writerMut is to synchronize writers. there can only be one writer at any time.
	writerMut *sync.Mutex

	flushCond *sync.Cond

	flusherDone chan bool
	errChan     chan error

	logTimeout time.Duration

	flushErr error
	flushedN int
	closed   bool
	closedCh chan struct{}

	broker *Broker[uint64]
}

func newBufferedLogWriter(size int, totalWritten, prevLSN uint64, w SegmentWriter) *BufferedLogWriter {
	sm := &sync.Mutex{}
	b := NewBroker[uint64]()
	go b.Start()

	return &BufferedLogWriter{
		buf:          make([]byte, size),
		flushBuf:     make([]byte, size),
		errChan:      make(chan error),
		closedCh:     make(chan struct{}),
		w:            w,
		swapMut:      sm,
		flushCond:    sync.NewCond(sm),
		bufMut:       &sync.Mutex{},
		writerMut:    &sync.Mutex{},
		logTimeout:   time.Millisecond * 8,
		totalWritten: totalWritten,
		latestInBuf:  prevLSN,
		broker:       b,
	}
}

func OpenBufferedLogWriter(bufSize int, segmentSize uint64, dir string) (*BufferedLogWriter, error) {
	sr, err := NewSegmentFS().OpenSegmentReader(dir, segmentSize)
	if err != nil {
		return nil, err
	}

	totalSegmentSize, err := sr.Size()
	if err != nil {
		return nil, err
	}

	prevLSN := uint64(0)
	r := OpenBufferedLogReader(dir, segmentSize)
	if err := r.RepairWAL(); err != nil {
		return nil, err
	}

	l, err := r.LastLSN()
	if err != nil && !errors.Is(err, ErrNoSegmentFile) {
		return nil, err
	} else {
		prevLSN = totalSegmentSize - uint64(len(l)) - uint64(LogRecordHeaderSize)
	}

	sw, err := NewSegmentFS().OpenSegmentWriter(dir, segmentSize)
	if err != nil {
		return nil, err
	}

	return newBufferedLogWriter(bufSize, totalSegmentSize, prevLSN, sw), nil
}

func (w *BufferedLogWriter) Write(log []byte) (lsn uint64, err error) {
	w.writerMut.Lock()
	defer w.writerMut.Unlock()

	w.bufMut.Lock()
	if w.closed {
		w.bufMut.Unlock()
		return 0, ErrWriterClosed
	}

	// TODO: do this without allocation
	sizeWithHeader := len(log) + LogRecordHeaderSize
	var d = make([]byte, sizeWithHeader)

	writeLogRecordHeader(LogRecordHeader{
		Size:    uint16(len(log)),
		PrevLSN: w.latestInBuf,
	}, d)

	copy(d[LogRecordHeaderSize:], log)

	size := len(d)
	if size <= w.available() {
		if n := copy(w.buf[w.offset:], d); n != size {
			panic("short write")
		}

		w.offset += size
		w.totalWritten += uint64(size)
		lsn = w.totalWritten - uint64(size)
		w.latestInBuf = lsn
		w.bufComplete = true
		w.bufMut.Unlock()

		return lsn, nil
	}

	acc := 0
	for {
		n := copy(w.buf[w.offset:], d[acc:])
		w.offset += n
		w.totalWritten += uint64(n)
		acc += n

		if size <= acc {
			lsn = w.totalWritten - uint64(size)
			w.latestInBuf = lsn
			w.bufComplete = true
			break
		}
		w.bufComplete = false
		w.bufMut.Unlock()
		if _, err := w.swap(); err != nil {
			return 0, err
		}
		w.bufMut.Lock()
	}

	// size is modified now hence return len(d)
	w.bufMut.Unlock()
	return lsn, nil
}

func (w *BufferedLogWriter) Flush() error {
	ch, err := w.swap()
	if err != nil {
		return err
	}

	err = <-ch
	return err
}

func (w *BufferedLogWriter) GetFlushedLSN() (uint64, error) {
	w.swapMut.Lock()
	defer w.swapMut.Unlock()

	// latestFlushed is protected by swapMut hence lock it before getting latest flushed.
	if w.latestFlushed == nil {
		return 0, errors.New("nothing is flushed")
	}

	return *w.latestFlushed, nil
}

func (w *BufferedLogWriter) Wait(lsn uint64) error {
	ch := w.broker.Subscribe()
	for {
		select {
		case latestFlushed := <-ch:
			if latestFlushed >= lsn {
				w.broker.Unsubscribe(ch)
				return nil
			}
		case <-w.closedCh:
			// NOTE: w.latestFlushed can be checked here without taking lock since writer is closed it cannot change
			// after this point
			if w.latestFlushed != nil && *w.latestFlushed >= lsn {
				return nil
			}

			return errors.New("writer was closed before flushing")
		}
	}
}

func (w *BufferedLogWriter) RunFlusher() {
	w.bufMut.Lock()
	if w.closed {
		panic("writer is closed")
	}
	w.bufMut.Unlock()

	w.swapMut.Lock()
	defer w.swapMut.Unlock()

	if w.flusherDone != nil {
		panic("flusher was already running")
	}

	w.flusherDone = make(chan bool)

	go func() {
		// NOTE: what happens when Flush is too slow compared to ticker period, does ticker events
		// start to queue up?
		ticker := time.NewTicker(w.logTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-w.flusherDone:
				log.Println("received flusherDone")

				flushErr, err := w.swap()
				if err != nil {
					w.errChan <- err
				}

				w.errChan <- <-flushErr
				return
			case <-ticker.C:
				if _, err := w.swap(); err != nil {
					w.Exit()
					return
				}
			}
		}
	}()
}

func (w *BufferedLogWriter) StopFlusher() error {
	// stop writers
	w.bufMut.Lock()
	w.closed = true
	w.bufMut.Unlock()

	close(w.closedCh)
	defer w.broker.Stop()

	// notify flusher
	w.flusherDone <- true

	return <-w.errChan
}

func (w *BufferedLogWriter) Exit() {
	// stop writers
	w.bufMut.Lock()
	w.closed = true
	w.bufMut.Unlock()

	close(w.closedCh)
	w.swapMut.Unlock()
	w.broker.Stop()
}

// available returns size of available space in current buffer in bytes.
func (w *BufferedLogWriter) available() int {
	return len(w.buf) - w.offset
}

func (w *BufferedLogWriter) swap() (flushErr chan error, swapErr error) {
	w.swapMut.Lock()

	// if flush failed before try flushing again synchronously
	if w.flushErr != nil {
		if err := w.flush(); err != nil {
			return nil, err
		}
	}

	w.bufMut.Lock()

	// NOTE: if buffer is empty do not request any io?
	// if w.offset == 0 {
	// 	w.bufMut.Unlock()
	// 	w.swapMut.Unlock()
	// 	flushErr = make(chan error, 1)
	// 	flushErr <- nil
	// 	return flushErr, nil
	// }

	// NOTE: if blocks were written padded, total written would be updated here for padded blocks.
	// w.totalWritten += uint64(len(w.buf)) - uint64(w.offset)

	// swap buffers

	w.buf, w.flushBuf = w.flushBuf, w.buf
	w.flushOffset = w.offset
	w.flushBufComplete = w.bufComplete

	// load lsn
	w.latestInFlushBuf = w.latestInBuf
	w.offset = 0
	w.bufMut.Unlock()

	flushErr = make(chan error, 1)
	go func() {
		defer w.swapMut.Unlock()
		flushErr <- w.flush()
	}()

	return flushErr, nil
}

func (w *BufferedLogWriter) flush() error {
	// NOTE: do not release lock on errors. lock should only be released when write succeeds to avoid missing writes.
	n, err := w.w.Write(w.flushBuf[w.flushedN:w.flushOffset], w.latestInFlushBuf)
	if err != nil {
		w.flushErr = err
		w.flushedN = n
		return err
	}

	cpy := w.latestInFlushBuf
	w.latestFlushed = &cpy

	w.flushErr = nil
	w.flushedN = 0
	w.broker.Publish(*w.latestFlushed)
	return nil
}
