package common

import (
	"sync"
)

type Stats struct {
	avg    map[string]float64
	counts map[string]int
	mu     sync.Mutex
}

func NewStats() *Stats {
	return &Stats{
		avg:    map[string]float64{},
		counts: map[string]int{},
		mu:     sync.Mutex{},
	}
}

func (s *Stats) Avg(key string, val float64) {
	s.mu.Lock()
	s.counts[key] += 1
	s.avg[key] += val
	s.mu.Unlock()
}
