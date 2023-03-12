package common

import (
	"sync"
	"sync/atomic"
)

var mutexPool sync.Pool

// init initializes mutexPool
func init() {
	mutexPool = sync.Pool{New: func() any {
		return &sync.Mutex{}
	}}
}

type KeyMutex[T any] struct {
	mutexes sync.Map
	gcLock  sync.Mutex
	counter uint64
}

// Lock acquires a lock for the given key and returns a releaser function. Caller should call releaser after
// it is done with the lock.
func (m *KeyMutex[T]) Lock(key T) func() {
	m.gcLock.Lock()
	defer m.gcLock.Unlock()

	// call gc every 1000th call.
	if c := atomic.AddUint64(&m.counter, 1); c%1000 == 0 {
		m.gc()
	}

	// mutexes are drawn from a pool to reduce gc pressure
	value, _ := m.mutexes.LoadOrStore(key, mutexPool.Get())
	mtx := value.(*sync.Mutex)
	mtx.Lock()

	return func() { mtx.Unlock() }
}

// gc runs every nth call to Lock and garbage collects unlocked mutexes to avoid mutexes map to get fatter infinitely.
func (m *KeyMutex[T]) gc() {
	collected, total := 0, 0
	m.mutexes.Range(func(key, value any) bool {
		total++
		if mut := value.(*sync.Mutex); mut.TryLock() {
			m.mutexes.Delete(key)
			mut.Unlock()
			mutexPool.Put(mut)
			collected++
		}

		return true
	})

	// log.Printf("KeyMutex: total: %v, collected: %v\n", total, collected)
}
