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
	"sync/atomic"
	"time"
)

var ErrPageNotFoundInPageMap = errors.New("page cannot be found in the page map")
var ErrRLockFailed = errors.New("RLock cannot be acquired on page")

type IBufferPool interface {
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
	page *pages.RawPage
}

var _ IBufferPool = &BufferPool{}

type BufferPool struct {
	poolSize        int
	frames          []*frame
	pageMap         map[uint64]int // physical page_id => frame index which keeps that page
	emptyFrames     []int          // list of indexes that points to empty frames in the pool
	Replacer        IReplacer
	DiskManager     disk.IDiskManager
	lock            sync.Mutex
	emptyFramesLock sync.Mutex
	logManager      *wal.LogManager
	fl              freelist.FreeList
}

func (b *BufferPool) GetFreeList() freelist.FreeList {
	return b.fl
}

func (b *BufferPool) FreePage(txn transaction.Transaction, pageId uint64, log bool) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	// TODO: do not return error from this method panic instead?
	if frame, ok := b.pageMap[pageId]; ok {
		frame := b.frames[frame]
		// this check might not be necessary, maybe assert and panic since this will be an internal call
		if frame.page.PinCount > 1 {
			return fmt.Errorf("freeing a pinned page, pin count: %v", frame.page.PinCount)
		}
	}

	if log {
		b.logManager.AppendLog(wal.NewFreePageLogRecord(txn.GetID(), pageId))
	}

	b.fl.Add(txn, pageId)
	return nil
}

func (b *BufferPool) GetPage(pageId uint64) (*pages.RawPage, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.getPage(pageId)
}

func (b *BufferPool) getPage(pageId uint64) (*pages.RawPage, error) {
	if frameId, ok := b.pageMap[pageId]; ok {
		b.pin(pageId)
		return b.frames[frameId].page, nil
	}

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if emptyFrameIdx := b.reserveFrame(); emptyFrameIdx >= 0 {
		if b.frames[emptyFrameIdx] == nil {
			b.frames[emptyFrameIdx] = &frame{pages.NewRawPage(pageId)}
		}

		p := b.frames[emptyFrameIdx].page
		// read page and put it inside the frame
		if err := b.DiskManager.ReadPage(pageId, p.GetWholeData()); err != nil {
			b.unReserveFrame(emptyFrameIdx)
			return nil, err
		}

		p.PageId = pageId

		b.pageMap[pageId] = emptyFrameIdx
		b.pin(pageId)

		return p, nil
	}

	// else choose a victim. write victim to disk if it is dirty. read new page and pin it.
	victimFrameIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	p := b.frames[victimFrameIdx].page
	if err := b.DiskManager.ReadPage(pageId, p.GetWholeData()); err != nil {
		return nil, err
	}

	p.PageId = pageId
	b.pageMap[pageId] = victimFrameIdx
	b.pin(pageId)
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

	return b.unpin(pageId, isDirty)
}

func (b *BufferPool) unpin(pageId uint64, isDirty bool) bool {
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		panic(fmt.Sprintf("unpinned a page which does not exist: %v", pageId))
	}

	// if found set its dirty field
	frame := b.frames[frameIdx]
	if isDirty {
		frame.page.SetDirty()
	}
	if frame.page.GetPageId() != pageId {
		panic("page id is not same")
	}
	// if pin count is already 0 it is already unpinned. Although that should not happen I guess
	if frame.page.GetPinCount() <= 0 {
		panic(fmt.Sprintf("buffer.Unpin is called while pin count is lte zero. PageId: %v, pin count %v\n", pageId, frame.page.GetPinCount()))
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
	defer b.lock.Unlock() // TODO: no need to hold lock when doing io.

	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		return ErrPageNotFoundInPageMap
	}
	frame := b.frames[frameIdx]

	if !frame.page.TryRLatch() {
		return ErrRLockFailed
	}
	defer frame.page.RUnLatch()

	if !frame.page.IsDirty() {
		return nil
	}

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

	b.lock.Lock()
	pooledPages := make([]uint64, 0)
	for _, frame := range b.frames {
		if frame.page != nil {
			pooledPages = append(pooledPages, frame.page.GetPageId())
		}
	}
	b.lock.Unlock()

	// flush all dirty pages. if TryFlush fails wait some time and try again.
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
	b.lock.Lock()
	defer b.lock.Unlock()

	if newPageId := b.fl.Pop(txn); newPageId != 0 {
		p, err := b.getPage(newPageId) // TODO: it is a new page no need to read its content from disk
		if err != nil {
			return nil, err
		}

		lsn := b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), p.GetPageId()))
		p.SetPageLSN(lsn)
		p.SetDirty()
		return p, nil
	}

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
		return p, nil
	}

	// if there is no empty frame, evict one and put new page to there.
	victimIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	p := b.frames[victimIdx].page
	p.Clear()

	p.PageId = newPageId
	b.pageMap[newPageId] = victimIdx
	b.pin(newPageId)

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

var s = time.Now()
var count uint64 = 0

// evictVictim chooses a victim page, writes its data to disk if it is dirty and returns emptied frame's index.
func (b *BufferPool) evictVictim() (int, error) {
	victimFrameIdx, err := b.Replacer.ChooseVictim()
	if err != nil {
		return 0, err
	}

	victim := b.frames[victimFrameIdx]
	if victim.page.GetPinCount() != 0 {
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.page.GetPinCount(), victim.page.GetPageId()))
	}

	victimPageId := victim.page.GetPageId()
	if victim.page.IsDirty() {
		if n := atomic.AddUint64(&count, 1); n%100 == 0 {
			log.Printf("eps: %v", float64(count)/time.Since(s).Seconds())
			atomic.AddUint64(&count, -n)
			s = time.Now()
		}

		// if log records for the victim page is not flushed, force flush log manager.
		if victim.page.GetPageLSN() > b.logManager.GetFlushedLSN() {
			if err := b.logManager.Flush(); err != nil {
				return 0, err
			}
		}

		data := victim.page.GetWholeData()
		if err := b.DiskManager.WritePage(data, victimPageId); err != nil {
			return 0, err
		}
	}

	delete(b.pageMap, victimPageId)

	return victimFrameIdx, nil
}

func NewBufferPool(dbFile string, poolSize int) *BufferPool {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}
	d, _, err := disk.NewDiskManager(dbFile)
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
	}

	flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1))
	if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
		log.Fatal("database cannot be created", err)
	}

	bp.fl = freelist.NewFreeList(transaction.TxnTODO(), &pager{bp}, bp.logManager, true)
	return bp
}

func NewBufferPoolWithDM(init bool, poolSize int, dm disk.IDiskManager, logManager *wal.LogManager) *BufferPool {
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
	}
	if init {
		flHeaderP := pages.InitSlottedPage(pages.NewRawPage(1))
		if err := bp.DiskManager.WritePage(flHeaderP.GetWholeData(), flHeaderP.GetPageId()); err != nil {
			log.Fatal("database cannot be created", err)
		}
	}

	bp.fl = freelist.NewFreeList(transaction.TxnTODO(), &pager{bp}, bp.logManager, init)
	return bp
}
