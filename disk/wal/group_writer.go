package wal

import (
	"errors"
	"fmt"
	"helin/common"
	"io"
	"log"
	"sync"
	"time"
)

type GroupWriter struct {
	buf         []byte
	offset      int
	flushBuf    []byte
	flushOffset int
	w           io.Writer
	mut         sync.Mutex

	timeout time.Duration

	flusherDone chan bool
	errChan     chan error
}

func NewGroupWriter(size int, w io.Writer) *GroupWriter {
	return &GroupWriter{
		buf:      make([]byte, size),
		flushBuf: make([]byte, size),
		errChan:  make(chan error),
		w:        w,
		mut:      sync.Mutex{},
		timeout:  time.Second,
	}
}

func (w *GroupWriter) Write(d []byte) (int, error) {
	size := len(d)
	if size <= w.Available() {
		copy(w.buf[w.offset:], d)
		w.offset += size
		return size, nil
	}

	acc := 0
	for {
		n := copy(w.buf[w.offset:], d[acc:])
		w.offset += n
		acc += n

		if size <= acc {
			break
		}
		w.swap()
	}

	// size is modified now hence return len(d)
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
	w.buf, w.flushBuf = w.flushBuf, w.buf
	w.flushOffset = w.offset
	w.offset = 0

	go func() {
		// TODO: how to handle error other than logging.
		if err := w.flush(true); err != nil {
			log.Printf("wal.GroupWriter flush failed: %v\n", err.Error())
		}
	}()
}

func (w *GroupWriter) flush(release bool) error {
	// NOTE: do not release lock on errors. lock should only be released when write succeeds to avoid missing writes.
	n, err := w.w.Write(w.flushBuf[:w.flushOffset])
	if err != nil {
		return err
	}
	if n != w.flushOffset {
		return errors.New("short write")
	}

	if release {
		w.mut.Unlock()
	}
	return nil
}

func (w *GroupWriter) swapAndWaitFlush() error {
	w.buf, w.flushBuf = w.flushBuf, w.buf
	w.flushOffset = w.offset
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
	w.offset = 0

	if err := w.flush(false); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	return nil
}
