package wal

import (
	"bytes"
	"github.com/stretchr/testify/require"
	"helin/common"
	"helin/disk/pages"
	"math/rand"
	"testing"
	"time"
)

// loggedSlottedPage is a poc logging implementation for pages.SlottedPage
type loggedSlottedPage struct {
	pages.SlottedPage
	logs []*LogRecord
}

func (p *loggedSlottedPage) InsertAt(idx int, data []byte) error {
	p.logs = append(p.logs, NewInsertLogRecord(0, uint16(idx), data, 0))
	return p.SlottedPage.InsertAt(idx, data)
}

func (p *loggedSlottedPage) SetAt(idx int, data []byte) error {
	old := common.Clone(p.GetAt(idx))
	p.logs = append(p.logs, NewSetLogRecord(1, uint16(idx), data, old, 0))
	return p.SlottedPage.SetAt(idx, data)
}

func (p *loggedSlottedPage) DeleteAt(idx int) error {
	deleted := common.Clone(p.GetAt(idx))
	p.logs = append(p.logs, NewDeleteLogRecord(0, uint16(idx), deleted, 0))
	return p.SlottedPage.DeleteAt(idx)
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func RandBytes(n int) []byte {
	return []byte(RandString(n))
}

func newP() loggedSlottedPage {
	p := pages.InitSlottedPage(pages.NewRawPage(1, 4096))
	return loggedSlottedPage{
		SlottedPage: p,
	}
}

func clone(p loggedSlottedPage) loggedSlottedPage {
	cp := pages.CastSlottedPage(&pages.RawPage{
		Data: common.Clone(p.GetWholeData()),
	})
	return loggedSlottedPage{
		SlottedPage: cp,
		logs:        common.Clone(p.logs),
	}
}

func rev(p loggedSlottedPage, logIdx int) {
	log := p.logs[logIdx]

	switch log.T {
	case TypeDelete:
		common.PanicIfErr(p.InsertAt(int(log.Idx), log.OldPayload))
	case TypeSet:
		d := p.GetAt(int(log.Idx))
		if bytes.Compare(d, log.Payload) != 0 {
			panic("payload is different than logged")
		}
		common.PanicIfErr(p.SetAt(int(log.Idx), log.OldPayload))
	case TypeInsert:
		d := p.GetAt(int(log.Idx))
		if bytes.Compare(d, log.Payload) != 0 {
			panic("payload is different than logged")
		}
		common.PanicIfErr(p.DeleteAt(int(log.Idx)))
	}

}

func TestWal(t *testing.T) {
	p := newP()
	steps := make([]loggedSlottedPage, 0)
	for i := 0; i < 10; i++ {
		b := RandBytes(10)
		require.NoError(t, p.InsertAt(i, b))
		steps = append(steps, clone(p))
	}

	logIdx := len(p.logs) - 1

	for _, step := range common.Reverse(steps) {
		if !step.Equals(&p.SlottedPage) {
			panic("not equals")
		}
		rev(p, logIdx)
		logIdx--
	}
}

func TestWalRandom(t *testing.T) {
	for x := 0; x < 10; x++ {
		p := newP()
		steps := make([]loggedSlottedPage, 0)
		for i := 0; i < 10; i++ {
			b := RandBytes(10)
			require.NoError(t, p.InsertAt(i, b))
			steps = append(steps, clone(p))
		}

		for i := 0; i < 1000; i++ {
			r := rand.Intn(3)
			idx := 0
			if p.GetHeader().SlotArrSize > 0 {
				idx = rand.Intn(int(p.GetHeader().SlotArrSize))
			} else {
				r = 0
			}

			switch r {
			case 0:
				b := RandBytes(10)
				if err := p.InsertAt(idx, b); err != nil {
					t.Logf("insert failed: %v", err.Error())
				}
			case 1:
				b := RandBytes(10)
				require.NoError(t, p.SetAt(idx, b))
			case 2:
				require.NoError(t, p.DeleteAt(idx))
			}

			cloned := clone(p)
			steps = append(steps, clone(p))

			logIdx := len(p.logs) - 1
			for _, step := range common.Reverse(steps) {
				if !step.Equals(&cloned.SlottedPage) {
					t.Fatal("not equals")
				}
				rev(cloned, logIdx)
				logIdx--
			}
		}
	}
}
