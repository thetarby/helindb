package common

import "sync"

type Event struct {
	mu *sync.Mutex
	c  *sync.Cond
}

func (e *Event) Wait() {
	e.mu.Lock()
	e.c.Wait()
	e.mu.Unlock()
}

func (e *Event) Broadcast() {
	e.c.Broadcast()
}

func NewEvent() *Event {
	m := &sync.Mutex{}
	return &Event{
		mu: m,
		c:  sync.NewCond(m),
	}
}
