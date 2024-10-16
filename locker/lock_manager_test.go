package locker

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLockManager(t *testing.T) {
	t.Run("lock acquisition and release", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		if err := lm.AcquireLock(1, 100, SharedLock); err != nil {
			t.Fatalf("Failed to acquire read lock: %v", err)
		}

		if err := lm.AcquireLock(1, 101, SharedLock); err != nil {
			t.Fatalf("Failed to acquire second read lock: %v", err)
		}

		lm.ReleaseLock(1, 100)
		lm.ReleaseLock(1, 101)

		if err := lm.AcquireLock(1, 102, ExclusiveLock); err != nil {
			t.Fatalf("Failed to acquire write lock after releasing read locks: %v", err)
		}

		lm.ReleaseLock(1, 102)
	})

	t.Run("lock upgrade", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		err := lm.AcquireLock(1, 100, SharedLock)
		if err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}

		err = lm.AcquireLock(1, 100, ExclusiveLock)
		if err != nil {
			t.Fatalf("Failed to upgrade to write lock: %v", err)
		}

		lm.ReleaseLock(1, 100)

		assert.Panics(t, func() {
			lm.ReleaseLock(1, 100)
		})
	})

	t.Run("deadlock when two read owners tries to upgrade ", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		err := lm.AcquireLock(1, 100, SharedLock)
		if err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}

		err = lm.AcquireLock(1, 101, SharedLock)
		if err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}

		wg := sync.WaitGroup{}
		wg.Add(2)
		errs := [2]error{}
		go func() {
			defer wg.Done()
			errs[0] = lm.AcquireLock(1, 100, ExclusiveLock)
			if errs[0] != nil {
				lm.ReleaseLock(1, 100)
			}
		}()

		go func() {
			defer wg.Done()
			errs[1] = lm.AcquireLock(1, 101, ExclusiveLock)
			if errs[1] != nil {
				lm.ReleaseLock(1, 101)
			}
		}()

		wg.Wait()

		// assert one has succeeded
		if errs[0] == nil && errs[1] == nil {
			t.Error("botch acquired lock")
		}

		// assert one has failed
		if errs[0] != nil && errs[1] != nil {
			t.Error("both failed to acquire lock")
		}
	})

	t.Run("concurrent shared lock", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		var wg sync.WaitGroup
		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id uint64) {
				defer wg.Done()

				if err := lm.AcquireLock(1, id, SharedLock); err != nil {
					t.Errorf("Goroutine %d failed to acquire lock: %v", id, err)
					return
				}

				time.Sleep(time.Second)
				lm.ReleaseLock(1, id)
			}(uint64(i))
		}

		wg.Wait()
	})

	t.Run("deadlock detection", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		// Create a simple deadlock scenario
		var wg sync.WaitGroup
		wg.Add(2)

		if err := lm.AcquireLock(1, 100, ExclusiveLock); err != nil {
			return
		}
		if err := lm.AcquireLock(2, 101, ExclusiveLock); err != nil {
			return
		}

		errs := [2]error{}
		go func() {
			defer wg.Done()

			err := lm.AcquireLock(2, 100, ExclusiveLock)
			if err != nil {
				lm.ReleaseLocks(100)
			}
			errs[0] = err

		}()

		go func() {
			defer wg.Done()

			err := lm.AcquireLock(1, 101, ExclusiveLock)
			if err != nil {
				lm.ReleaseLocks(100)
			}
			errs[1] = err
		}()

		wg.Wait()

		// assert one has succeeded
		if errs[0] == nil && errs[1] == nil {
			t.Error("botch acquired lock")
		}

		// assert one has failed
		if errs[0] != nil && errs[1] != nil {
			t.Error("botch failed to acquire lock")
		}
	})

	t.Run("x is granted when txn has x", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		if err := lm.AcquireLock(1, 100, ExclusiveLock); err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}

		if err := lm.AcquireLock(1, 100, ExclusiveLock); err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}
	})

	t.Run("s is granted when txn has x", func(t *testing.T) {
		lm := NewLockManager()
		defer lm.Stop()

		if err := lm.AcquireLock(1, 100, ExclusiveLock); err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}

		if err := lm.AcquireLock(1, 100, SharedLock); err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}

		if err := lm.AcquireLock(1, 101, SharedLock); err != nil {
			t.Fatalf("Failed to acquire initial read lock: %v", err)
		}
	})
}

func parallelReader(m *LockManager, id uint64, clocked, cunlock, cdone chan bool) {
	if err := m.AcquireLock(1, id, SharedLock); err != nil {
		panic(err)
	}

	clocked <- true
	<-cunlock
	m.ReleaseLock(1, id)
	cdone <- true
}

func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	m := NewLockManager()
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(m, nextId(), clocked, cunlock, cdone)
	}
	// Wait for all parallel RLock()s to succeed.
	for i := 0; i < numReaders; i++ {
		<-clocked
	}
	for i := 0; i < numReaders; i++ {
		cunlock <- true
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numReaders; i++ {
		<-cdone
	}
}

func TestParallelReaders(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

func reader(m *LockManager, id uint64, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if err := m.AcquireLock(1, id, SharedLock); err != nil {
			panic(err)
		}
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			m.ReleaseLock(1, id)
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		m.ReleaseLock(1, id)
	}
	cdone <- true
}

func writer(m *LockManager, id uint64, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		if err := m.AcquireLock(1, id, ExclusiveLock); err != nil {
			panic(err)
		}

		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			m.ReleaseLock(1, id)
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		m.ReleaseLock(1, id)
	}
	cdone <- true
}

var currID uint64

func nextId() uint64 {
	return atomic.AddUint64(&currID, 1)
}

func HammerRWMutex(gomaxprocs, numReaders, num_iterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	rwm := NewLockManager()
	cdone := make(chan bool)
	go writer(rwm, nextId(), num_iterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(rwm, nextId(), num_iterations, &activity, cdone)
	}
	go writer(rwm, nextId(), num_iterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(rwm, nextId(), num_iterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

func TestRWMutex(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 5
	}
	HammerRWMutex(1, 1, n)
	HammerRWMutex(1, 3, n)
	HammerRWMutex(1, 10, n)
	HammerRWMutex(4, 1, n)
	HammerRWMutex(4, 3, n)
	HammerRWMutex(4, 10, n)
	HammerRWMutex(10, 1, n)
	HammerRWMutex(10, 3, n)
	HammerRWMutex(10, 10, n)
	HammerRWMutex(10, 5, n)
	HammerRWMutex(100, 5, n)
	HammerRWMutex(1000, 5, n)
	HammerRWMutex(1000, 50, n)
}
