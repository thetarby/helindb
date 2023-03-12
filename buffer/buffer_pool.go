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

type Pool interface {
	GetPage(pageId uint64) (*pages.RawPage, error)
	Unpin(pageId uint64, isDirty bool) bool
	FlushAll() error

	// NewPage creates a new page
	NewPage(txn transaction.Transaction) (page *pages.RawPage, err error)

	// FreePage deletes a page from the buffer pool. Returns error if the page exists but could not be deleted and
	// panics if page does not exist
	FreePage(txn transaction.Transaction, pageId uint64, log bool)

	// EmptyFrameSize returns the number empty frames which does not hold data of any physical page
	EmptyFrameSize() int
}

type frame struct {
	page *pages.RawPage
}

var _ Pool = &BufferPool{}

type BufferPool struct {
	poolSize        int
	frames          []*frame
	pageMap         map[uint64]int // physical page_id => frame index which keeps that page
	emptyFrames     []int          // list of indexes that points to empty frames in the pool
	Replacer        IReplacer
	DiskManager     disk.IDiskManager
	lock            sync.Mutex
	emptyFramesLock sync.Mutex
	logManager      wal.LogManager
	fl              freelist.FreeList
	opLocks         *common.KeyMutex[uint64]
}

func (b *BufferPool) GetFreeList() freelist.FreeList {
	return b.fl
}

func (b *BufferPool) FreePage(txn transaction.Transaction, pageId uint64, log bool) {
	b.lock.Lock()
	if frame, ok := b.pageMap[pageId]; ok {
		frame := b.frames[frame]
		if frame.page.PinCount > 1 {
			b.lock.Unlock()
			panic(fmt.Sprintf("freeing a pinned page, pin count: %v", frame.page.PinCount))
		}
	}
	b.lock.Unlock()

	if log {
		b.logManager.AppendLog(wal.NewFreePageLogRecord(txn.GetID(), pageId))
	}

	b.fl.Add(txn, pageId)
}

func (b *BufferPool) GetPage(pageId uint64) (*pages.RawPage, error) {
	b.lock.Lock()

	r := b.opLocks.Lock(pageId)
	defer r()

	if frameId, ok := b.pageMap[pageId]; ok {
		b.pin(pageId)
		b.lock.Unlock()
		return b.frames[frameId].page, nil
	}

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if emptyFrameIdx := b.reserveFrame(); emptyFrameIdx >= 0 {
		if b.frames[emptyFrameIdx] == nil {
			b.frames[emptyFrameIdx] = &frame{pages.NewRawPage(pageId)}
		}

		b.pageMap[pageId] = emptyFrameIdx
		b.pin(pageId)
		b.lock.Unlock()

		// return frame waiter that waits until io is complete
		p := b.frames[emptyFrameIdx].page
		// read page and put it inside the frame
		if err := b.DiskManager.ReadPage(pageId, p.GetWholeData()); err != nil {
			b.lock.Lock()
			delete(b.pageMap, pageId)
			b.frames[emptyFrameIdx].page.DecrPinCount()
			b.unReserveFrame(emptyFrameIdx)
			b.lock.Unlock()
			return nil, err
		}

		p.PageId = pageId

		return p, nil
	}

	b.lock.Unlock()
	// else choose a victim. write victim to disk if it is dirty. read new page and pin it.
	victimFrameIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	b.lock.Lock()
	// it is already pinned by evictVictim hence only update page map
	b.pageMap[pageId] = victimFrameIdx
	p := b.frames[victimFrameIdx].page
	p.PageId = pageId
	b.lock.Unlock()

	// wait until io finishes
	if err := b.DiskManager.ReadPage(pageId, p.GetWholeData()); err != nil {
		b.lock.Lock()
		delete(b.pageMap, pageId)
		b.frames[victimFrameIdx].page.DecrPinCount()
		b.unReserveFrame(victimFrameIdx)
		b.lock.Unlock()
		return nil, fmt.Errorf("ReadPage failed: %w", err)
	}
	return p, err
}

// pin increments page's pin count and pins the frame that keeps the page to avoid it being chosen as victim
func (b *BufferPool) pin(pageId uint64) {
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

func (b *BufferPool) Unpin(pageId uint64, isDirty bool) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		panic(fmt.Sprintf("unpinned a page which does not exist: %v", pageId))
	}

	return b.unpinFrame(frameIdx, isDirty)
}

func (b *BufferPool) unpinFrame(frameIdx int, isDirty bool) bool {
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
func (b *BufferPool) TryFlush(pageId uint64) error {
	b.lock.Lock()

	r := b.opLocks.Lock(pageId)
	defer r()

	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		b.lock.Unlock()
		return ErrPageNotFoundInPageMap
	}

	frame := b.frames[frameIdx]

	if !frame.page.TryRLatch() {
		b.lock.Unlock()
		return ErrRLockFailed
	}

	defer frame.page.RUnLatch()

	if !frame.page.IsDirty() {
		b.lock.Unlock()
		return nil
	}

	// pinning frame so that frame will not be replaced while flushing to disk.
	b.pin(pageId)
	defer b.unpinFrame(frameIdx, false)

	b.lock.Unlock()

	// if log records for the victim page is not flushed, force flush log manager.
	if frame.page.GetPageLSN() > b.logManager.GetFlushedLSN() {
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
func (b *BufferPool) FlushAll() error {
	if err := b.logManager.Flush(); err != nil {
		return err
	}

	// take a list of all pages in the page map at the time of calling.
	b.lock.Lock()
	pooledPages := make([]uint64, 0)
	for pid := range b.pageMap {
		pooledPages = append(pooledPages, pid)
	}
	b.lock.Unlock()

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

func (b *BufferPool) NewPage(txn transaction.Transaction) (page *pages.RawPage, err error) {
	// first try to pop from free list
	if newPageId := b.fl.Pop(txn); newPageId != 0 {
		p, err := b.GetPage(newPageId) // TODO: it is a new page no need to read its content from disk
		if err != nil {
			b.fl.Add(txn, newPageId)
			return nil, err
		}

		lsn := b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), p.GetPageId()))
		p.SetPageLSN(lsn)
		p.SetDirty()
		return p, nil
	}

	// else create a new page
	b.lock.Lock()
	newPageId := b.DiskManager.NewPage()

	// if there is any empty frame, put new page to there.
	if emptyFrameIdx := b.reserveFrame(); emptyFrameIdx >= 0 {
		if b.frames[emptyFrameIdx] == nil {
			b.frames[emptyFrameIdx] = &frame{pages.NewRawPage(newPageId)}
		}

		p := b.frames[emptyFrameIdx].page

		b.pageMap[newPageId] = emptyFrameIdx
		b.pin(newPageId)
		lsn := b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), p.GetPageId()))
		p.SetPageLSN(lsn)
		p.SetDirty()
		b.lock.Unlock()
		return p, nil
	}

	b.lock.Unlock()

	// if there is no empty frame, evict one and put new page to there.
	victimIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	b.lock.Lock()
	defer b.lock.Unlock()
	p := b.frames[victimIdx].page
	p.Clear()

	p.PageId = newPageId
	b.pageMap[newPageId] = victimIdx

	lsn := b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), p.GetPageId()))
	p.SetPageLSN(lsn)
	p.SetDirty()
	return p, nil
}

func (b *BufferPool) EmptyFrameSize() int {
	return len(b.emptyFrames)
}

func (b *BufferPool) reserveFrame() int {
	b.emptyFramesLock.Lock()
	defer b.emptyFramesLock.Unlock()

	if len(b.emptyFrames) > 0 {
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]
		return emptyFrameIdx
	}

	return -1
}

func (b *BufferPool) unReserveFrame(idx int) {
	b.emptyFramesLock.Lock()
	defer b.emptyFramesLock.Unlock()

	b.emptyFrames = append(b.emptyFrames, idx)
}

// evictVictim chooses a victim page, writes its data to disk if it is dirty and returns emptied frame's index.
func (b *BufferPool) evictVictim() (int, error) {
	b.lock.Lock()
	victimFrameIdx, err := b.Replacer.ChooseVictim()
	if err != nil {
		b.lock.Unlock()
		return 0, err
	}

	victim := b.frames[victimFrameIdx]
	if victim.page.GetPinCount() != 0 {
		b.lock.Unlock()
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.page.GetPinCount(), victim.page.GetPageId()))
	}

	r := b.opLocks.Lock(victim.page.GetPageId())
	defer r()

	// pin evicting frame to avoid replacer to pick it up again before io completes.
	victim.page.IncrPinCount()
	b.Replacer.Pin(victimFrameIdx)

	victimPageId := victim.page.GetPageId()
	// on fail waiter should roll back page map changes
	delete(b.pageMap, victimPageId)
	b.lock.Unlock()

	if victim.page.IsDirty() {
		// if log records for the victim page is not flushed, force flush log manager.
		if victim.page.GetPageLSN() > b.logManager.GetFlushedLSN() {
			if err := b.logManager.Flush(); err != nil {
				// on error roll back state
				b.lock.Lock()
				victim.page.DecrPinCount()
				b.Replacer.Unpin(victimFrameIdx)
				b.pageMap[victimPageId] = victimFrameIdx
				b.lock.Unlock()
				return 0, err
			}
		}

		data := victim.page.GetWholeData()
		if err := b.DiskManager.WritePage(data, victimPageId); err != nil {
			// on error roll back state
			b.lock.Lock()
			victim.page.DecrPinCount()
			b.Replacer.Unpin(victimFrameIdx)
			b.pageMap[victimPageId] = victimFrameIdx
			b.lock.Unlock()
			return 0, err
		}
	}

	return victimFrameIdx, nil
}

func NewBufferPool(dbFile string, poolSize int) *BufferPool {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}
	d, _, err := disk.NewDiskManager(dbFile, false)
	common.PanicIfErr(err)
	bp := &BufferPool{
		poolSize:    poolSize,
		frames:      make([]*frame, poolSize),
		pageMap:     map[uint64]int{},
		emptyFrames: emptyFrames,
		Replacer:    NewClockReplacer(poolSize),
		DiskManager: d,
		lock:        sync.Mutex{},
		logManager:  wal.NewLogManager(io.Discard),
		opLocks:     &common.KeyMutex[uint64]{},
	}

	flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1))
	if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
		log.Fatal("database cannot be created", err)
	}

	bp.fl = freelist.NewFreeList(transaction.TxnTODO(), bp, bp.logManager, true)
	return bp
}

func NewBufferPoolWithDM(init bool, poolSize int, dm disk.IDiskManager, logManager wal.LogManager) *BufferPool {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}

	if logManager == nil {
		logManager = wal.NewLogManager(io.Discard)
	}

	bp := &BufferPool{
		poolSize:        poolSize,
		frames:          make([]*frame, poolSize),
		pageMap:         map[uint64]int{},
		emptyFrames:     emptyFrames,
		Replacer:        NewClockReplacer(poolSize),
		DiskManager:     dm,
		lock:            sync.Mutex{},
		emptyFramesLock: sync.Mutex{},
		logManager:      logManager,
		fl:              nil,
		opLocks:         &common.KeyMutex[uint64]{},
	}
	if init {
		flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1))
		if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
			log.Fatal("database cannot be created", err)
		}
	}

	bp.fl = freelist.NewFreeList(transaction.TxnTODO(), bp, bp.logManager, init)
	return bp
}
