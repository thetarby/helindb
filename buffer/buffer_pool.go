package buffer

import (
	"errors"
	"fmt"
	"helin/common"
	"helin/disk"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/freelist"
	"helin/transaction"
	"io"
	"log"
	"sync"
	"time"
)

var ErrPageNotFoundInPageMap = errors.New("page cannot be found in the page map")
var ErrRLockFailed = errors.New("RLock cannot be acquired on page")
var ErrFlushFailed = errors.New("flushing page has failed")

type Pool interface {
	GetPage(pageId uint64) (*pages.RawPage, error)
	Unpin(pageId uint64, isDirty bool) bool
	FlushAll() error

	// NewPage creates a new page
	NewPage(txn transaction.Transaction) (page *pages.RawPage, err error)

	// FreePage deletes a page from the buffer pool. Returns error if the page exists but could not be deleted and
	// panics if page does not exist
	FreePage(txn transaction.Transaction, pageId uint64, log bool) error

	// EmptyFrameSize returns the number empty frames which does not hold data of any physical page
	EmptyFrameSize() int

	GetFreeList() freelist.FreeList
}

type frame struct {
	page              *pages.RawPage
	evicting, reading uint64
	pool              *PoolV2
	lock              *sync.Mutex
	err               error
	idx               int
}

var _ Pool = &PoolV1{}

type PoolV1 struct {
	poolSize    int
	frames      []*frame
	pageMap     map[uint64]int // physical page_id => frame index which keeps that page
	emptyFrames []int          // list of indexes that points to empty frames in the pool
	Replacer    IReplacer
	DiskManager disk.IDiskManager
	logManager  wal.LogManager
	fl          freelist.FreeList

	// xLock is exclusive lock which should be acquired before changing buffer pool's state. State of buffer pool
	// includes pin count of frames, pageMap and emptyFrames. Before changing any of those, xLock must be acquired.
	xLock sync.Mutex

	// slowPath is acquired when thread requires an io. It is used to avoid blocking other threads on xLock
	// while doing io. This also means that at most one thread might request an io operation at the same
	// time.
	slowPath sync.Mutex

	evicting uint64
}

func (b *PoolV1) GetFreeList() freelist.FreeList {
	return b.fl
}

func (b *PoolV1) FreePage(txn transaction.Transaction, pageId uint64, log bool) error {
	b.xLock.Lock()
	if frame, ok := b.pageMap[pageId]; ok {
		frame := b.frames[frame]
		if frame.page.PinCount > 1 {
			b.xLock.Unlock()
			panic(fmt.Sprintf("freeing a pinned page, pin count: %v", frame.page.PinCount))
		}
	}
	b.xLock.Unlock()

	if err := b.fl.Add(txn.GetID(), pageId); err != nil {
		return err
	}

	return nil
}

func (b *PoolV1) GetPage(pageId uint64) (*pages.RawPage, error) {
	// happy path: take x lock assuming page is already in pool, if it is, return directly.
	b.xLock.Lock()
	if frameId, ok := b.pageMap[pageId]; ok {
		b.pin(pageId)
		b.xLock.Unlock()
		return b.frames[frameId].page, nil
	}
	b.xLock.Unlock()

	// slow path requires at least 1 io.
	b.slowPath.Lock()
	defer b.slowPath.Unlock()

	// need to check pool again, since another thread might have fetched it, while this one blocks on slowPath lock
	b.xLock.Lock()
	if frameId, ok := b.pageMap[pageId]; ok {
		b.pin(pageId)
		b.xLock.Unlock()
		return b.frames[frameId].page, nil
	}
	b.xLock.Unlock()

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if emptyFrameIdx := b.reserveFrame(); emptyFrameIdx >= 0 {
		p := b.frames[emptyFrameIdx].page
		// read page and put it inside the frame
		if err := b.DiskManager.ReadPage(pageId, p.GetWholeData()); err != nil {
			b.releaseFrame(emptyFrameIdx)
			return nil, err
		}

		p.PageId = pageId

		b.xLock.Lock()
		defer b.xLock.Unlock()
		b.pageMap[pageId] = emptyFrameIdx

		return p, nil
	}

	// else choose a victim. write victim to disk if it is dirty. read new page and pin it.
	victimFrameIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	// it is already pinned by evictVictim hence only update page map
	p := b.frames[victimFrameIdx].page
	p.PageId = pageId

	// wait until io finishes
	if err := b.DiskManager.ReadPage(pageId, p.GetWholeData()); err != nil {
		b.releaseFrame(victimFrameIdx)
		return nil, fmt.Errorf("ReadPage failed: %w", err)
	}

	b.xLock.Lock()
	defer b.xLock.Unlock()
	b.pageMap[pageId] = victimFrameIdx

	return p, err
}

// pin increments page's pin count and pins the frame that keeps the page to avoid it being chosen as victim
func (b *PoolV1) pin(pageId uint64) {
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		// NOTE: is panic ok here? this method is private and should not be called with a non-existent
		// pageID hence panic might be ok?
		panic(fmt.Sprintf("pinned a page which does not exist: %v", pageId))
	}

	frame := b.frames[frameIdx]
	frame.page.IncrPinCount()
	b.Replacer.Pin(frameIdx)
}

func (b *PoolV1) Unpin(pageId uint64, isDirty bool) bool {
	b.xLock.Lock()
	defer b.xLock.Unlock()

	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		panic(fmt.Sprintf("unpinned a page which does not exist: %v", pageId))
	}

	return b.unpinFrame(frameIdx, isDirty)
}

func (b *PoolV1) unpinFrame(frameIdx int, isDirty bool) bool {
	// if found set its dirty field
	frame := b.frames[frameIdx]
	if isDirty {
		frame.page.SetDirty()
	}

	// if pin count is already 0 it is already unpinned. Although that should not happen I guess
	if frame.page.GetPinCount() <= 0 {
		panic(fmt.Sprintf("buffer.Unpin is called while pin count is lte zero. PageId: %v, pin count %v\n", frame.page.GetPageId(), frame.page.GetPinCount()))
	}

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
func (b *PoolV1) TryFlush(pageId uint64) error {
	// if below two lines is commented out there is no need for b.evicting field;
	// since slowPath is already acquired, it is guaranteed that there is no io being done in parallel. Hence,
	// if a page is not found in pageMap, it means it is already flushed to disk.
	//b.slowPath.Lock()
	//defer b.slowPath.Unlock()

	b.xLock.Lock()
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		b.xLock.Unlock()
		return ErrPageNotFoundInPageMap
	}

	if b.evicting == pageId {
		return ErrRLockFailed
	}

	// if page is still in pageMap, acquire shared lock on it and check if it is dirty. If so, flush,
	// otherwise return directly.
	frame := b.frames[frameIdx]
	if !frame.page.TryRLatch() {
		b.xLock.Unlock()
		return ErrRLockFailed
	}
	defer frame.page.RUnLatch()

	if !frame.page.IsDirty() {
		b.xLock.Unlock()
		return nil
	}

	// pinning frame so that frame will not be chosen victim again while flushing to disk.
	b.pin(pageId)
	defer b.unpinFrame(frameIdx, false)

	b.xLock.Unlock()

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

// FlushAll determines all dirty pages at the time of call and syncs all of them to disk. It blocks until all
// determined dirty pages are synced.
func (b *PoolV1) FlushAll() error {
	if err := b.logManager.Flush(); err != nil {
		return err
	}

	// take a list of all pages in the page map at the time of calling.
	b.xLock.Lock()
	pooledPages := make([]uint64, 0)
	for pid := range b.pageMap {
		pooledPages = append(pooledPages, pid)
	}
	b.xLock.Unlock()

	// flush all pages. if TryFlush fails wait some time and try again.
	for _, pid := range pooledPages {
		for {
			if err := b.TryFlush(pid); err != nil {
				if err == ErrPageNotFoundInPageMap {
					// if it is not in page map, it is already evicted and synced hence we can continue.
					break
				} else if err == ErrRLockFailed {
					time.Sleep(time.Microsecond)
					continue
				} else {
					return err
				}
			}
			break
		}
		time.Sleep(time.Millisecond * 4)
	}
	return nil
}

func (b *PoolV1) NewPage(txn transaction.Transaction) (page *pages.RawPage, err error) {
	availableFrameIdx := b.reserveFrame()
	if availableFrameIdx <= 0 {
		// if there is no empty frame, evict one and put new page to there.
		victimIdx, err := b.evictVictim()
		if err != nil {
			return nil, err
		}

		availableFrameIdx = victimIdx
	}

	newPageId, err := b.fl.Pop(txn.GetID())
	if err != nil {
		return nil, err
	}

	if newPageId == 0 {
		newPageId = b.DiskManager.NewPage()
	}

	b.xLock.Lock()
	defer b.xLock.Unlock()

	p := b.frames[availableFrameIdx].page

	p.PageId = newPageId
	b.pageMap[newPageId] = availableFrameIdx

	// TODO: new page log record?
	lsn := b.logManager.AppendLog(wal.NewDiskAllocPageLogRecord(txn.GetID(), p.GetPageId()))
	p.SetPageLSN(lsn)
	p.SetDirty()
	return p, nil
}

func (b *PoolV1) EmptyFrameSize() int {
	return len(b.emptyFrames)
}

func (b *PoolV1) reserveFrame() int {
	b.xLock.Lock()
	defer b.xLock.Unlock()

	if len(b.emptyFrames) > 0 {
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		// alloc frame if it is not yet
		if b.frames[emptyFrameIdx] == nil {
			b.frames[emptyFrameIdx] = newFrame(nil, emptyFrameIdx)
		}

		b.frames[emptyFrameIdx].page.IncrPinCount()
		b.Replacer.Pin(emptyFrameIdx)
		return emptyFrameIdx
	}

	return -1
}

func (b *PoolV1) releaseFrame(idx int) {
	b.xLock.Lock()
	defer b.xLock.Unlock()
	b.unpinFrame(idx, false)
	b.emptyFrames = append(b.emptyFrames, idx)
}

// evictVictim chooses a victim page, writes its data to disk if it is dirty and returns emptied frame's index.
func (b *PoolV1) evictVictim() (int, error) {
	b.xLock.Lock()
	victimFrameIdx, err := b.Replacer.ChooseVictim()
	if err != nil {
		b.xLock.Unlock()
		return 0, err
	}

	victim := b.frames[victimFrameIdx]
	if victim.page.GetPinCount() != 0 {
		b.xLock.Unlock()
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.page.GetPinCount(), victim.page.GetPageId()))
	}

	// pin evicting frame to avoid replacer to pick it up again before io completes.
	victim.page.IncrPinCount()
	b.Replacer.Pin(victimFrameIdx)

	victimPageId := victim.page.GetPageId()

	// delete evicting page from pageMap so that when xLock is released, if another thread tries to access this page
	// it will wait on slowPath until io completes.
	delete(b.pageMap, victimPageId)
	b.evicting = victimPageId

	b.xLock.Unlock()

	if victim.page.IsDirty() {
		// if log records for the victim page is not flushed, force flush log manager.
		if victim.page.GetPageLSN() > b.logManager.GetFlushedLSNOrZero() {
			if err := b.logManager.Flush(); err != nil {
				// on error roll back state
				b.xLock.Lock()
				victim.page.DecrPinCount()
				b.Replacer.Unpin(victimFrameIdx)
				b.pageMap[victimPageId] = victimFrameIdx
				b.evicting = 0
				b.xLock.Unlock()
				return 0, err
			}
		}

		data := victim.page.GetWholeData()
		if err := b.DiskManager.WritePage(data, victimPageId); err != nil {
			// on error roll back state
			b.xLock.Lock()
			victim.page.DecrPinCount()
			b.Replacer.Unpin(victimFrameIdx)
			b.pageMap[victimPageId] = victimFrameIdx
			b.evicting = 0
			b.xLock.Unlock()
			return 0, err
		}

		b.xLock.Lock()
		b.evicting = 0
		b.xLock.Unlock()
	}

	return victimFrameIdx, nil
}

func NewBufferPool(dbFile string, poolSize int) *PoolV1 {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}
	d, _, err := disk.NewDiskManager(dbFile, false)
	common.PanicIfErr(err)
	bp := &PoolV1{
		poolSize:    poolSize,
		frames:      make([]*frame, poolSize),
		pageMap:     map[uint64]int{},
		emptyFrames: emptyFrames,
		Replacer:    NewClockReplacer(poolSize),
		DiskManager: d,
		logManager:  wal.NewLogManager(io.Discard),
	}

	flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1))
	if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
		log.Fatal("database cannot be created", err)
	}

	bp.fl = freelist.NewFreeList(bp, bp.logManager, true)
	return bp
}

func NewBufferPoolWithDM(init bool, poolSize int, dm disk.IDiskManager, logManager wal.LogManager) *PoolV1 {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}

	if logManager == nil {
		logManager = wal.NewLogManager(io.Discard)
	}

	bp := &PoolV1{
		poolSize:    poolSize,
		frames:      make([]*frame, poolSize),
		pageMap:     map[uint64]int{},
		emptyFrames: emptyFrames,
		Replacer:    NewClockReplacer(poolSize),
		DiskManager: dm,
		logManager:  logManager,
		fl:          nil,
	}
	if init {
		flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1))
		if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
			log.Fatal("database cannot be created", err)
		}
	}

	bp.fl = freelist.NewFreeList(bp, bp.logManager, init)
	return bp
}
