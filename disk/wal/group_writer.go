package wal

import (
	"errors"
	"fmt"
	"helin/common"
	"helin/disk/pages"
	"io"
	"log"
	"sync"
	"time"
)

type LogWriter interface {
	Write(d []byte, lsn pages.LSN) (int, error)
}

type GroupWriter struct {
	buf         []byte
	offset      int
	latestInBuf pages.LSN

	flushBuf         []byte
	flushOffset      int
	latestInFlushBuf pages.LSN

	latestFlushed pages.LSN

	w      io.Writer
	mut    sync.Mutex
	bufMut sync.Mutex

	flushEvent *common.Event

	flusherDone chan bool
	errChan     chan error
	stats       *common.Stats
}

func NewGroupWriter(size int, w io.Writer) *GroupWriter {
	return &GroupWriter{
		buf:        make([]byte, size),
		flushBuf:   make([]byte, size),
		errChan:    make(chan error),
		w:          w,
		mut:        sync.Mutex{},
		stats:      common.NewStats(),
		flushEvent: common.NewEvent(),
		bufMut:     sync.Mutex{},
	}
}

func (w *GroupWriter) Write(d []byte, lsn pages.LSN) (int, error) {
	w.bufMut.Lock()
	size := len(d)
	if size <= w.Available() {
		copy(w.buf[w.offset:], d)
		w.offset += size
		w.latestInBuf = lsn
		w.bufMut.Unlock()
		return size, nil
	}

	acc := 0
	for {
		n := copy(w.buf[w.offset:], d[acc:])
		w.offset += n
		acc += n

		if size <= acc {
			w.latestInBuf = lsn
			break
		}
		w.bufMut.Unlock()
		w.swap()
		w.bufMut.Lock()
	}

	// size is modified now hence return len(d)
	w.bufMut.Unlock()
	return size, nil
}

// Available returns size of available space in current buffer in bytes.
func (w *GroupWriter) Available() int {
	return len(w.buf) - w.offset
}

func (w *GroupWriter) RunFlusher() {
	w.mut.Lock()
	defer w.mut.Unlock()

	if w.flusherDone != nil {
		panic("flusher was already running")
	}

	w.flusherDone = make(chan bool)

	go func() {
		// NOTE: what happens when Flush is too slow compared to ticker period, does ticker events
		// start to queue up?
		ticker := time.NewTicker(common.LogTimeout)
		defer ticker.Stop()

		for {
			select {
			case <-w.flusherDone:
				log.Println("received flusherDone")
				w.errChan <- w.swapAndWaitFlush()
				return
			case <-ticker.C:
				w.swap()
			}
		}
	}()
}

func (w *GroupWriter) StopFlusher() error {
	w.mut.Lock()
	defer w.mut.Unlock()
	if w.flusherDone == nil {
		// NOTE: maybe this should be allowed?
		panic("flusher is already stopped")
	}

	w.flusherDone <- true
	w.flusherDone = nil
	return <-w.errChan
}

func (w *GroupWriter) swap() {
	w.mut.Lock()
	w.bufMut.Lock()

	// swap buffers
	w.buf, w.flushBuf = w.flushBuf, w.buf
	w.flushOffset = w.offset

	// load lsn
	w.latestInFlushBuf = w.latestInBuf
	w.offset = 0
	w.bufMut.Unlock()

	go func() {
		// TODO: how to handle error other than logging.
		if err := w.flush(true); err != nil {
			log.Printf("wal.GroupWriter flush failed: %v\n", err.Error())
		}
	}()
}

func (w *GroupWriter) flush(release bool) error {
	// NOTE: do not release lock on errors. lock should only be released when write succeeds to avoid missing writes.
	w.stats.Avg("avg_log_flush_size", float64(w.flushOffset))
	n, err := w.w.Write(w.flushBuf[:w.flushOffset])
	if err != nil {
		return err
	}
	if n != w.flushOffset {
		return errors.New("short write")
	}

	w.latestFlushed = w.latestInFlushBuf
	if release {
		w.mut.Unlock()
	}

	w.flushEvent.Broadcast()
	return nil
}

func (w *GroupWriter) swapAndWaitFlush() error {
	w.buf, w.flushBuf = w.flushBuf, w.buf
	w.flushOffset = w.offset
	w.latestInFlushBuf = w.latestInBuf
	w.offset = 0

	if err := w.flush(false); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}

func (w *GroupWriter) SwapAndWaitFlush() error {
	w.mut.Lock()
	defer w.mut.Unlock()

	w.buf, w.flushBuf = w.flushBuf, w.buf
	w.flushOffset = w.offset
	w.latestInFlushBuf = w.latestInBuf
	w.offset = 0

	if err := w.flush(false); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}
