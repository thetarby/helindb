package bwal

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"slices"
	"testing"
)

func makeTmpDir(t *testing.T) string {
	dir, err := os.MkdirTemp("./testlog", "")
	if err != nil {
		panic(err)
	}

	t.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	})
	return dir
}

func initWAL(dir string, segmentSize uint64, bufSize int) *BufferedLogWriter {
	sw := NewSegmentWriter(dir, 0, segmentSize)
	lw := newBufferedLogWriter(bufSize, 0, sw)

	return lw
}

func TestWal_All_Inserted_Can_Be_Found(t *testing.T) {
	testCases := []struct {
		bufSize, segmentSize, logCount int
	}{
		{
			bufSize:     4096 * 16,
			segmentSize: 4096 * 16 * 16,
			logCount:    1_000_000,
		},
		{
			bufSize:     4096,
			segmentSize: 4096 * 2,
			logCount:    50_000,
		},
		{
			bufSize:     4096 * 2,
			segmentSize: 4096,
			logCount:    50_000,
		},
		{
			bufSize:     4096,
			segmentSize: 1000,
			logCount:    10_000,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			dir := makeTmpDir(t)

			bufSize, segmentSize := testCase.bufSize, testCase.segmentSize
			gw, err := OpenBufferedLogWriter(bufSize, uint64(segmentSize), dir)
			if err != nil {
				t.Fatal(err)
			}

			gw.RunFlusher()

			lsnToLog := map[uint64]string{}
			for i := 0; i < testCase.logCount; i++ {
				l := fmt.Sprintf("log_%v", i+1)

				lsn, err := gw.Write([]byte(l))
				lsnToLog[lsn] = l
				require.NoError(t, err)
			}

			require.NoError(t, gw.StopFlusher())

			br := OpenBufferedLogReader(dir, uint64(segmentSize))
			for lsn, log := range lsnToLog {
				l, err := br.SkipToLSN(lsn)
				require.NoError(t, err)

				assert.Equal(t, log, string(l))
			}
		})
	}
}

func TestWal_Next(t *testing.T) {
	testCases := []struct {
		bufSize, segmentSize, logCount int
	}{
		{
			bufSize:     4096 * 4,
			segmentSize: 4096 * 16,
			logCount:    50_000,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			dir := makeTmpDir(t)

			bufSize, segmentSize := testCase.bufSize, testCase.segmentSize
			gw, err := OpenBufferedLogWriter(bufSize, uint64(segmentSize), dir)
			if err != nil {
				t.Fatal(err)
			}

			gw.RunFlusher()

			logs := make([]string, 0)
			for i := 0; i < testCase.logCount; i++ {
				l := fmt.Sprintf("log_%v", i+1)

				_, err := gw.Write([]byte(l))
				logs = append(logs, l)
				require.NoError(t, err)
			}

			require.NoError(t, gw.StopFlusher())

			br := OpenBufferedLogReader(dir, uint64(segmentSize))
			i := 0
			for {
				l, err := br.Next()
				if err != nil {
					if errors.Is(err, ErrAtLast) {
						assert.Equal(t, len(logs), i)
						break
					} else {
						t.Fatal(err)
					}
				}

				assert.Equal(t, logs[i], string(l))
				i++
			}
		})
	}
}

func TestWal_Prev(t *testing.T) {
	testCases := []struct {
		bufSize, segmentSize, logCount int
	}{
		{
			bufSize:     4096 * 4,
			segmentSize: 4096 * 16,
			logCount:    50_000,
		},
	}

	for _, testCase := range testCases {
		t.Run("", func(t *testing.T) {
			dir := makeTmpDir(t)

			bufSize, segmentSize := testCase.bufSize, testCase.segmentSize
			gw, err := OpenBufferedLogWriter(bufSize, uint64(segmentSize), dir)
			if err != nil {
				t.Fatal(err)
			}

			gw.RunFlusher()

			logs := make([]string, 0)
			for i := 0; i < testCase.logCount; i++ {
				l := fmt.Sprintf("log_%v", i+1)

				_, err := gw.Write([]byte(l))
				logs = append(logs, l)
				require.NoError(t, err)
			}

			require.NoError(t, gw.StopFlusher())

			br := OpenBufferedLogReader(dir, uint64(segmentSize))
			_, err = br.LastLSN()
			assert.NoError(t, err)

			i := len(logs) - 2
			for {
				l, err := br.Prev()
				if err != nil {
					if errors.Is(err, ErrAtFirst) {
						assert.Equal(t, -1, i)
						break
					} else {
						t.Fatal(err)
					}
				}

				assert.Equal(t, logs[i], string(l))
				i--
			}
		})
	}
}

func TestBufferedLogReader_RepairWAL(t *testing.T) {
	dir := makeTmpDir(t)

	segmentSize, bufSize := uint64(8192*32), 8192

	lw := initWAL(dir, segmentSize, bufSize)

	lw.RunFlusher()

	// write logs
	for i := 0; i < 100_000; i++ {
		l := fmt.Sprintf("log_%v", i+1)

		_, err := lw.Write([]byte(l))
		require.NoError(t, err)
	}

	require.NoError(t, lw.StopFlusher())

	// truncate using segment reader so that last log is partial
	s := openSegmentReader(dir, segmentSize)
	origSize, err := s.Size()
	assert.NoError(t, err)

	err = s.Truncate(origSize - 1)
	assert.NoError(t, err)

	r := OpenBufferedLogReader(dir, segmentSize)
	assert.NoError(t, r.RepairWAL())

	// assert all found until last log
	for i := 0; i < 99_999; i++ {
		l, err := r.Next()
		assert.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("log_%v", i+1)), l)
	}

	_, err = r.Next()
	assert.ErrorIs(t, err, ErrAtLast)
}

func TestBufferedLogReader_TruncateUntil(t *testing.T) {
	// TODO: test different truncate sizes
	t.Run("next starts from correct log", func(t *testing.T) {
		dir := makeTmpDir(t)

		segmentSize, bufSize := uint64(8192*32), 8192

		lw := initWAL(dir, segmentSize, bufSize)

		lw.RunFlusher()

		// write logs
		type log struct {
			log string
			lsn uint64
		}
		var logs = make([]log, 0)

		for i := 0; i < 100_000; i++ {
			l := fmt.Sprintf("log_%v", i+1)

			lsn, err := lw.Write([]byte(l))
			require.NoError(t, err)

			logs = append(logs, log{
				log: l,
				lsn: lsn,
			})
		}

		require.NoError(t, lw.StopFlusher())

		lr := OpenBufferedLogReader(dir, segmentSize)
		assert.NoError(t, lr.TruncateUntil(logs[20_000].lsn))

		for _, log := range logs[20_000:] {
			l, err := lr.Next()
			assert.NoError(t, err)
			assert.Equal(t, []byte(log.log), l)
		}
	})

	t.Run("prev stops at correct log", func(t *testing.T) {
		dir := makeTmpDir(t)

		segmentSize, bufSize := uint64(8192*32), 8192

		lw := initWAL(dir, segmentSize, bufSize)

		lw.RunFlusher()

		// write logs
		type log struct {
			log string
			lsn uint64
		}
		var logs = make([]log, 0)

		for i := 0; i < 100_000; i++ {
			l := fmt.Sprintf("log_%v", i+1)

			lsn, err := lw.Write([]byte(l))
			require.NoError(t, err)

			logs = append(logs, log{
				log: l,
				lsn: lsn,
			})
		}

		require.NoError(t, lw.StopFlusher())

		lr := OpenBufferedLogReader(dir, segmentSize)
		assert.NoError(t, lr.TruncateUntil(logs[10_000].lsn))

		slices.Reverse(logs[10_000:])

		l, err := lr.LastLSN()
		assert.NoError(t, err)
		assert.Equal(t, []byte(logs[10_000].log), l)

		for _, log := range logs[10_001:] {
			l, err := lr.Prev()
			assert.NoError(t, err)
			assert.Equal(t, []byte(log.log), l)
		}

		_, err = lr.Prev()
		assert.ErrorIs(t, err, ErrAtFirst)
	})
}
