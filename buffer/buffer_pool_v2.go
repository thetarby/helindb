package buffer

import (
	"errors"
	"fmt"
	"helin/common"
	"helin/disk"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/freelist/pfreelistv1"
	"helin/locker"
	"helin/transaction"
	log2 "log"
	"sync"
	"time"
)

var _ Pool = &PoolV2{}

type PoolV2 struct {
	poolSize        int
	frames          []*frame
	pageMap         sync.Map // map[uint64]int physical page_id => frame index which keeps that page
	emptyFrames     []int    // list of indexes that points to empty frames in the pool
	Replacer        IReplacer
	DiskManager     disk.IDiskManager
	emptyFramesLock sync.Mutex
	logManager      wal.LogManager
	fl              FreeList

	// PoolV2 has only one global lock for its state. Threads does not hold lock when doing io. Io requests is issued
	// by frames in parallel. lock is only acquired when changing or checking pool's state, meaning, reserving,
	// releasing, pinning frames, updating or looking up pageMap.
	lock sync.Mutex

	// evictCond is fired after every eviction completes. If any thread should wait for an eviction to finish, it should
	// wait on this condition and check pageMap to see if the specific page it is waiting for is evicted or not.
	// Note that evictCond uses PoolV2.lock as its locker so that when Wait returns lock is acquired.
	evictCond *sync.Cond

	lockManager *locker.LockManager
}

func (b *PoolV2) GetFreeList() FreeList {
	return b.fl
}

func (b *PoolV2) FreePage(txn transaction.Transaction, pageId uint64) error {
	b.lock.Lock()
	if frameIdx, ok := b.get(pageId); ok {
		f := b.frames[frameIdx]
		common.AssertCallback(f.page.PinCount <= 1, fmt.Sprintf("freeing a pinned page, pin count: %v", f.page.PinCount), func() {
			b.lock.Unlock()
		})
	}
	b.lock.Unlock()

	if err := b.fl.Add(txn, pageId); err != nil {
		return err
	}

	return nil
}

func (b *PoolV2) GetPage(pageId uint64) (*pages.RawPage, error) {
	b.lock.Lock()

	// if already in frame return directly frame
	if frameId, ok := b.get(pageId); ok && frameId >= 0 {
		b.pin(pageId)
		b.lock.Unlock()

		if err := b.frames[frameId].Resolve(); err != nil {
			return nil, err
		}

		return b.frames[frameId].page, nil
	} else if frameId == -1 {
		// if it is evicting
		for {
			b.evictCond.Wait()
			if frameIdx, ok := b.get(pageId); !ok {
				// if page is not found in pageMap, it is evicted successfully. proceed to fetch it again.
				break
			} else if ok && frameIdx != -1 {
				// if eviction failed, then it is already in a frame, pin and directly return it
				b.pin(pageId)
				return b.frames[frameIdx].page, nil
			}
		}
	}

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if emptyFrameIdx := b.reserveFrame(); emptyFrameIdx >= 0 {
		b.set(pageId, emptyFrameIdx)

		frame := b.frames[emptyFrameIdx]
		frame.reading = pageId
		frame.page.PageId = pageId

		b.lock.Unlock()
		if err := frame.Resolve(); err != nil {
			return nil, err
		}

		return frame.page, nil
	}

	// chose victim frame
	victimFrameIdx, err := b.chooseVictimFrame()
	if err != nil {
		b.lock.Unlock()
		return nil, err
	}
	victim := b.frames[victimFrameIdx]
	victimPageId := victim.page.GetPageId()

	// update page map
	b.set(victimPageId, -1)
	b.set(pageId, victimFrameIdx)

	// update frame
	victim.reading = pageId
	victim.evicting = victimPageId
	victim.page.PageId = pageId

	b.lock.Unlock()

	if err := victim.Resolve(); err != nil {
		return nil, err
	}

	return victim.page, err
}

// pin increments page's pin count and pins the frame that keeps the page to avoid it being chosen as victim
func (b *PoolV2) pin(pageId uint64) {
	frameIdx, ok := b.get(pageId)
	common.Assert(ok, fmt.Sprintf("pinned a page which does not exist: %v", pageId))

	frame := b.frames[frameIdx]
	frame.page.IncrPinCount()
	b.Replacer.Pin(frameIdx)
}

func (b *PoolV2) Unpin(pageId uint64, isDirty bool) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	frameIdx, ok := b.get(pageId)
	common.Assert(ok, fmt.Sprintf("unpinned a page which does not exist: %v", pageId))

	return b.unpinFrame(frameIdx, isDirty)
}

func (b *PoolV2) unpinFrame(frameIdx int, isDirty bool) bool {
	// if found set its dirty field
	frame := b.frames[frameIdx]
	if isDirty {
		frame.page.SetDirty()
	}

	// if pin count is already 0 it is already unpinned.
	common.Assert(frame.page.GetPinCount() > 0, fmt.Sprintf("buffer.Unpin is called while pin count is lte zero. PageId: %v, pin count %v\n", frame.page.GetPageId(), frame.page.GetPinCount()))

	// decrease pin count and if it is 0 unpin frame in the replacer so that new pages can be read
	frame.page.DecrPinCount()
	if frame.page.GetPinCount() == 0 {
		b.Replacer.Unpin(frameIdx)
		return true
	}

	return false
}

// TryFlush tries to take a read lock on page and syncs its content to disk. If it fails to lock the page it
// returns ErrRLockFailed. If page is not dirty directly returns.
func (b *PoolV2) TryFlush(pageId uint64) error {
	b.lock.Lock()

	frameIdx, ok := b.get(pageId)
	if !ok {
		b.lock.Unlock()
		return ErrPageNotFoundInPageMap
	}
	if frameIdx == -1 {
		// if it is already evicting, wait eviction to finish
		for {
			b.evictCond.Wait()
			if frameIdx, ok := b.get(pageId); !ok {
				// if page is removed from pageMap it is safe to assume that page is flushed
				break
			} else if ok && frameIdx != -1 {
				// if page is still in pageMap but not in evicting status then eviction may have failed
				// or page might be fetched from disk again, but we cannot be sure hence should return error.
				b.lock.Unlock()
				return ErrFlushFailed
			}
		}

		b.lock.Unlock()
		return nil
	}

	// pinning frame so that frame will not be replaced while flushing to disk.
	b.pin(pageId)

	b.lock.Unlock()

	frame := b.frames[frameIdx]
	if err := frame.Resolve(); err != nil {
		return err
	}

	// unpinFrame is deferred after Resolve because if resolve fails it unpins
	defer func() {
		b.lock.Lock()
		defer b.lock.Unlock()
		b.unpinFrame(frameIdx, false)
	}()

	// try shared locking page
	if !b.lockManager.TryAcquireLatch(frame.page.GetPageId(), 0, locker.SharedLock) {
		return ErrRLockFailed
	}
	defer b.lockManager.ReleaseLatch(frame.page.GetPageId(), 0)

	if !frame.page.IsDirty() {
		return nil
	}

	// if log records for the victim page is not flushed, force flush log manager.
	if frame.page.GetPageLSN() > b.logManager.GetFlushedLSNOrZero() {
		if err := b.logManager.Flush(); err != nil {
			return err
		}
	}

	if err := b.DiskManager.WritePage(frame.page.GetWholeData(), frame.page.GetPageId()); err != nil {
		return err
	}
	frame.page.SetClean()
	return nil
}

// FlushAll2 determines all dirty pages at the time of call and syncs all of them to disk. It blocks until all
// determined dirty pages are synced.
func (b *PoolV2) FlushAll2() error {
	if err := b.logManager.Flush(); err != nil {
		return err
	}

	// take a list of all pages in the page map at the time of calling.
	b.lock.Lock()
	pooledPages := make([]uint64, 0)
	b.pageMap.Range(func(key, value any) bool {
		pooledPages = append(pooledPages, key.(uint64))
		return true
	})

	b.lock.Unlock()

	// flush all pages. if TryFlush fails wait some time and try again.
	for _, pid := range pooledPages {
		for {
			if err := b.TryFlush(pid); err != nil {
				if errors.Is(err, ErrPageNotFoundInPageMap) {
					// if it is not in page map, it is already evicted and synced hence we can continue.
					break
				} else if errors.Is(err, ErrRLockFailed) {
					time.Sleep(time.Microsecond)
					continue
				} else {
					return err
				}
			}
			break
		}
		// time.Sleep(time.Millisecond * 4)
	}
	return nil
}

func (b *PoolV2) FlushAll() error {
	if err := b.logManager.Flush(); err != nil {
		return err
	}

	// take a list of all pages in the page map at the time of calling.
	b.lock.Lock()
	pooledPages := make([]uint64, 0)
	b.pageMap.Range(func(key, value any) bool {
		pooledPages = append(pooledPages, key.(uint64))
		return true
	})

	b.lock.Unlock()

	if len(pooledPages) == 0 {
		return nil
	}

	// flush in parallel maybe like this?
	wg := sync.WaitGroup{}
	errMut := sync.Mutex{}
	errs := make([]error, 0)
	chunkSize := (len(pooledPages) / 512) + 1
	for _, chunk := range common.Chunks(pooledPages, chunkSize) {
		wg.Add(1)

		go func(arr []uint64) {
			defer wg.Done()

			for _, pid := range arr {
				for {
					if err := b.TryFlush(pid); err != nil {
						if errors.Is(err, ErrPageNotFoundInPageMap) {
							// if it is not in page map, it is already evicted and synced hence we can continue.
							break
						} else if errors.Is(err, ErrRLockFailed) {
							time.Sleep(time.Microsecond)
							continue
						} else {
							errMut.Lock()
							errs = append(errs, err)
							errMut.Unlock()
							return
						}
					}
					break
				}
				// time.Sleep(time.Millisecond * 4)
			}
		}(chunk)
	}
	wg.Wait()

	return errors.Join(errs...)
}

func (b *PoolV2) NewPage(txn transaction.Transaction) (page *pages.RawPage, err error) {
	// try to pop from free list, if popped fetch and use it
	newPageId, err := b.fl.Pop(txn)
	if err != nil {
		return nil, err
	}
	if newPageId != 0 {
		return b.GetPage(newPageId)
	}

	// else reserve a frame and alloc new page using disk manager
	b.lock.Lock()
	availableFrameIdx := b.reserveFrame()
	if availableFrameIdx <= 0 {
		// if there is no empty frame, evict one and put new page to there.
		victimFrameIdx, err := b.chooseVictimFrame()
		if err != nil {
			b.lock.Unlock()
			return nil, err
		}

		availableFrameIdx = victimFrameIdx
		victimPageID := b.frames[availableFrameIdx].page.GetPageId()
		b.frames[availableFrameIdx].evicting = victimPageID
		b.set(victimPageID, -1)
	}

	newPageId = b.DiskManager.NewPage()
	b.logManager.AppendLog(txn, wal.NewDiskAllocPageLogRecord(txn.GetID(), newPageId))

	frame := b.frames[availableFrameIdx]

	frame.page.PageId = newPageId
	b.set(newPageId, availableFrameIdx)

	b.lock.Unlock()
	if err := frame.Resolve(); err != nil {
		return nil, err
	}

	return frame.page, nil
}

func (b *PoolV2) EmptyFrameSize() int {
	return len(b.emptyFrames)
}

func (b *PoolV2) WithFL(fl FreeList) *PoolV2 {
	b.fl = fl
	return b
}

// reserveFrame tries to find an empty frame and pins it if found, should be called after lock is acquired
func (b *PoolV2) reserveFrame() int {
	if len(b.emptyFrames) > 0 {
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		// alloc frame if it is not yet
		if b.frames[emptyFrameIdx] == nil {
			b.frames[emptyFrameIdx] = newFrame(b, emptyFrameIdx)
		}

		b.frames[emptyFrameIdx].page.IncrPinCount()
		b.Replacer.Pin(emptyFrameIdx)
		return emptyFrameIdx
	}

	return -1
}

// releaseFrame should be called after lock is acquired
func (b *PoolV2) releaseFrame(idx int) {
	b.unpinFrame(idx, false)
	b.emptyFrames = append(b.emptyFrames, idx)
}

// chooseVictimFrame should be called after lock is acquired
func (b *PoolV2) chooseVictimFrame() (int, error) {
	// chose victim frame
	victimFrameIdx, err := b.Replacer.ChooseVictim()
	if err != nil {
		b.lock.Unlock()
		return 0, err
	}

	// do some logic assertions
	victim := b.frames[victimFrameIdx]
	common.AssertCallback(victim.page.GetPinCount() == 0, fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.page.GetPinCount(), victim.page.GetPageId()), func() {
		b.lock.Unlock()
	})
	common.AssertCallback(victim.evicting == 0 && victim.reading == 0, fmt.Sprintf("a frame is chosen as victim while it is not resolved"), func() {
		b.lock.Unlock()
	})

	// pin selected victim frame
	victim.page.IncrPinCount()
	b.Replacer.Pin(victimFrameIdx)
	return victimFrameIdx, nil
}

func (b *PoolV2) get(pageId uint64) (int, bool) {
	v, ok := b.pageMap.Load(pageId)
	if !ok {
		return 0, false
	}

	return v.(int), ok
}

func (b *PoolV2) set(pageId uint64, frameIdx int) {
	b.pageMap.Store(pageId, frameIdx)
}

func (b *PoolV2) del(pageId uint64) {
	b.pageMap.Delete(pageId)
}

func NewBufferPoolV2WithDM(init bool, poolSize int, dm disk.IDiskManager, logManager wal.LogManager, locker *locker.LockManager) *PoolV2 {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}

	bp := &PoolV2{
		poolSize:        poolSize,
		frames:          make([]*frame, poolSize),
		pageMap:         sync.Map{},
		emptyFrames:     emptyFrames,
		Replacer:        NewClockReplacer(poolSize),
		DiskManager:     dm,
		lock:            sync.Mutex{},
		emptyFramesLock: sync.Mutex{},
		logManager:      logManager,
		lockManager:     locker,
		fl:              nil,
	}
	if init {
		// TODO: convert not logged
		flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1, disk.PageSize))
		if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
			log2.Fatal("database cannot be created", err)
		}
	}

	bp.fl = pfreelistv1.NewBPFreeList(bp, bp.logManager, init)
	bp.evictCond = sync.NewCond(&bp.lock)

	return bp
}

func (f *frame) resolve() error {
	if f.evicting != 0 {
		if f.page.IsDirty() {
			// if log records for the victim page is not flushed, force flush log manager.
			if f.page.GetPageLSN() > f.pool.logManager.GetFlushedLSNOrZero() {
				if err := f.pool.logManager.Flush(); err != nil {
					f.pool.lock.Lock()
					f.evicting = 0
					f.pool.set(f.evicting, f.idx)
					f.pool.unpinFrame(f.idx, false)
					f.pool.lock.Unlock()
					f.pool.evictCond.Broadcast()
					return err
				}
			}

			data := f.page.GetWholeData()
			if err := f.pool.DiskManager.WritePage(data, f.evicting); err != nil {
				f.pool.lock.Lock()
				f.evicting = 0
				f.pool.set(f.evicting, f.idx)
				f.pool.unpinFrame(f.idx, false)
				f.pool.lock.Unlock()
				f.pool.evictCond.Broadcast()
				return err
			}
		}

		f.pool.lock.Lock()
		f.page.SetClean()
		f.pool.del(f.evicting)
		f.evicting = 0
		f.pool.lock.Unlock()
		f.pool.evictCond.Broadcast()
	}

	if f.reading != 0 {
		if err := f.pool.DiskManager.ReadPage(f.reading, f.page.GetWholeData()); err != nil {
			f.pool.lock.Lock()
			f.pool.del(f.reading)
			f.pool.releaseFrame(f.idx)
			f.reading = 0
			f.pool.lock.Unlock()
			return err
		}
		f.reading = 0
	}

	return nil
}

func (f *frame) Resolve() error {
	f.lock.Lock()
	defer f.lock.Unlock()

	f.err = f.resolve()
	return f.err
}

func newFrame(p *PoolV2, idx int) *frame {
	return &frame{
		page:     pages.NewRawPage(0, disk.PageSize),
		evicting: 0,
		reading:  0,
		pool:     p,
		lock:     &sync.Mutex{},
		err:      nil,
		idx:      idx,
	}
}
