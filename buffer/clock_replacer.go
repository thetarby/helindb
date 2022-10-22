package buffer

import (
	"errors"
	"sync"
)

type counter struct {
	pinned  bool
	counter uint8
}

var _ IReplacer = &ClockReplacer{}

type ClockReplacer struct {
	frames         []counter
	victimIterator int
	lock           sync.Mutex
}

func (c *ClockReplacer) Pin(frameId int) {
	c.frames[frameId].pinned = true
	c.frames[frameId].counter = 1
}

func (c *ClockReplacer) Unpin(frameId int) {
	if !c.frames[frameId].pinned {
		panic("unpinning a page which is already unpinned or not pinned at all")
	}

	c.frames[frameId].pinned = false
}

func (c *ClockReplacer) ChooseVictim() (frameId int, err error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	st := c.victimIterator
	pass := 0
	for {
		f := c.frames[c.victimIterator]
		if !f.pinned {
			if f.counter == 1 {
				c.frames[c.victimIterator].counter = 0
			} else if f.counter == 0 {
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
		if frame.pinned {
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
