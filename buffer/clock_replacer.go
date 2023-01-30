package buffer

import (
	"errors"
	"sync"
)

const (
	PinnedBit       uint8 = 1 << 7
	SecondChanceBit uint8 = 1 << 6
)

type counter struct {
	bits uint8
}

var _ IReplacer = &ClockReplacer{}

type ClockReplacer struct {
	frames         []counter
	victimIterator int
	lock           sync.Mutex // NOTE: is this needed? access to buffer pool is already synchronized right now.
}

func (c *ClockReplacer) Pin(frameId int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.frames[frameId].bits |= PinnedBit
	c.frames[frameId].bits |= SecondChanceBit
}

func (c *ClockReplacer) Unpin(frameId int) {
	c.lock.Lock()
	defer c.lock.Unlock()

	if (c.frames[frameId].bits & PinnedBit) == 0 {
		panic("unpinning a page which is already unpinned or not pinned at all")
	}

	c.frames[frameId].bits &= ^PinnedBit
}

func (c *ClockReplacer) ChooseVictim() (frameId int, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	st := c.victimIterator
	pass := 0
	for {
		f := c.frames[c.victimIterator]
		if f.bits&PinnedBit == 0 {
			if f.bits&SecondChanceBit > 0 {
				c.frames[c.victimIterator].bits &= ^SecondChanceBit
			} else if f.bits&SecondChanceBit == 0 {
				victim := c.victimIterator
				c.victimIterator = (c.victimIterator + 1) % c.GetSize()
				return victim, nil
			}
		}

		c.victimIterator = (c.victimIterator + 1) % c.GetSize()

		if c.victimIterator == st {
			if pass == 0 {
				pass++
			} else if pass == 1 {
				return 0, errors.New("nothing is unpinned")
			}
		}
	}
}

func (c *ClockReplacer) GetSize() int {
	return len(c.frames)
}

func (c *ClockReplacer) NumPinnedPages() int {
	i := 0
	for _, frame := range c.frames {
		if frame.bits&PinnedBit > 0 {
			i++
		}
	}

	return i
}

func NewClockReplacer(size int) *ClockReplacer {
	return &ClockReplacer{
		frames: make([]counter, size, size),
		lock:   sync.Mutex{},
	}
}
