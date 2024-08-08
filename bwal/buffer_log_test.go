package bwal

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

type failingSegmentWriter struct {
	buf            *os.File
	failed         bool
	previousFailed bool
	behaviour      string
}

func (f *failingSegmentWriter) Read(p []byte) (n int, err error) {
	return f.buf.Read(p)
}

func (f *failingSegmentWriter) Seek(offset int64, whence int) (int64, error) {
	return f.buf.Seek(offset, whence)
}

func (f *failingSegmentWriter) Truncate(size uint64) error {
	return f.buf.Truncate(int64(size))
}

func (f *failingSegmentWriter) TruncateFront(size uint64) error {
	panic("implement me")
}

func (f *failingSegmentWriter) LastSegmentHeader() (*SegmentHeader, error) {
	return &SegmentHeader{
		LastLSN: 0,
		StartAt: 0,
	}, nil
}

func (f *failingSegmentWriter) FirstSegmentHeader() (*SegmentHeader, error) {
	return &SegmentHeader{
		LastLSN: 0,
		StartAt: 0,
	}, nil
}

func (f *failingSegmentWriter) Size() (uint64, error) {
	s, err := f.buf.Stat()
	if err != nil {
		return 0, err
	}

	return uint64(s.Size()), nil
}

func (f *failingSegmentWriter) FrontTruncatedSize() (uint64, error) {
	return 0, nil
}

func (f *failingSegmentWriter) Write(block []byte, lastLsn uint64) (int, error) {
	n := rand.Intn(150)
	switch f.behaviour {
	case "fail_always_after_first":
		if n == 1 || f.failed {
			f.failed = true
			return 0, errors.New("failed")
		}
	case "fail_occasionally":
		if n == 1 && !f.previousFailed {
			f.previousFailed = true
			return 0, errors.New("failed")
		}
	default:
		panic("not implemented")
	}

	return f.buf.Write(block)
}

func (f *failingSegmentWriter) Close() error {
	return f.buf.Close()
}

var _ SegmentWriter = &failingSegmentWriter{}
var _ SegmentReader = &failingSegmentWriter{}

func TestBufferedLogWriter_Write(t *testing.T) {
	type testCase struct {
		writerCount int
		logCount    int
		retryCount  int
	}

	for _, tc := range []testCase{
		{
			writerCount: 35,
			logCount:    100_000,
			retryCount:  20,
		},
	} {
		for i := 0; i < tc.retryCount; i++ {
			t.Run("", func(t *testing.T) {
				dir := makeTmpDir(t)
				f, err := os.OpenFile(filepath.Join(dir, "test"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, DefaultOptions.FilePerms)
				assert.NoError(t, err)

				sw := &failingSegmentWriter{buf: f, behaviour: "fail_always_after_first"}
				lw := newBufferedLogWriter(8192, 0, sw)
				lw.RunFlusher()

				type log struct {
					log string
					lsn uint64
				}

				logsMut := &sync.Mutex{}
				var logs = make([]log, 0)

				ch := make(chan string, tc.logCount)

				go func() {
					for i := 0; i < tc.logCount; i++ {
						l := fmt.Sprintf("log_%v", i+1)

						ch <- l
					}

				}()

				wg := sync.WaitGroup{}
				for i := 0; i < tc.writerCount; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							msg := <-ch

							lsn, err := lw.Write([]byte(msg))
							if err != nil {
								return
							}

							if err := lw.Wait(lsn); err != nil {
								return
							}

							logsMut.Lock()
							logs = append(logs, log{
								log: msg,
								lsn: lsn,
							})
							logsMut.Unlock()
						}
					}()
				}

				wg.Wait()
				if len(logs) == 0 {
					t.Log("no logs written")
					return
				}
				t.Logf("%v log is written", len(logs))

				// open another descriptor since writer process can still write things at the end of the file
				// and change file cursor's position
				rf, err := os.OpenFile(filepath.Join(dir, "test"), os.O_RDWR, DefaultOptions.FilePerms)
				assert.NoError(t, err)

				br := BufferedLogReader{
					segmentReader: &failingSegmentWriter{buf: rf},
				}

				assert.NoError(t, br.RepairWAL())

				for _, lo := range logs {
					l, err := br.SkipToLSN(lo.lsn)
					if err != nil {
						t.Log(lo.lsn)
						t.Fatal(err)
					}

					assert.Equal(t, lo.log, string(l))
				}
			})
		}
	}
}

func TestBufferedLogWriter_Write_Retry(t *testing.T) {
	type testCase struct {
		writerCount int
		logCount    int
		retryCount  int
	}

	for _, tc := range []testCase{
		{
			writerCount: 35,
			logCount:    100_000,
			retryCount:  5,
		},
	} {
		for i := 0; i < tc.retryCount; i++ {
			t.Run("", func(t *testing.T) {
				dir := makeTmpDir(t)
				f, err := os.OpenFile(filepath.Join(dir, "test"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, DefaultOptions.FilePerms)
				assert.NoError(t, err)

				sw := &failingSegmentWriter{buf: f, behaviour: "fail_occasionally"}
				lw := newBufferedLogWriter(8192*4, 0, sw)
				lw.RunFlusher()

				type log struct {
					log string
					lsn uint64
				}

				logsMut := &sync.Mutex{}
				var logs = make([]log, 0)

				ch := make(chan string, tc.logCount)

				go func() {
					for i := 0; i < tc.logCount; i++ {
						l := fmt.Sprintf("log_%v", i+1)

						ch <- l
					}
					close(ch)

				}()

				wg := sync.WaitGroup{}
				for i := 0; i < tc.writerCount; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						for msg := range ch {
							lsn, err := lw.Write([]byte(msg))
							if err != nil {
								panic(err)
							}

							if err := lw.Wait(lsn); err != nil {
								panic(err)
							}

							logsMut.Lock()
							logs = append(logs, log{
								log: msg,
								lsn: lsn,
							})
							logsMut.Unlock()
						}
					}()
				}

				wg.Wait()

				// open another descriptor since writer process can still write things at the end of the file
				// and change file cursor's position
				rf, err := os.OpenFile(filepath.Join(dir, "test"), os.O_RDWR, DefaultOptions.FilePerms)
				assert.NoError(t, err)

				br := BufferedLogReader{
					segmentReader: &failingSegmentWriter{buf: rf},
				}

				for _, l := range logs {
					writtenLog, err := br.SkipToLSN(l.lsn)
					assert.NoError(t, err)
					assert.Equal(t, l.log, string(writtenLog))
				}
			})
		}
	}
}
