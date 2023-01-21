package buffer

import (
	"errors"
	"fmt"
	"helin/common"
	"helin/disk"
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
	"io"
	"log"
	"sort"
	"sync"
)

type IBufferPool interface {
	GetPage(pageId uint64) (*pages.RawPage, error)
	pin(pageId uint64)
	Unpin(pageId uint64, isDirty bool) bool
	Flush(pageId uint64) error
	FlushAll() error

	// NewPage creates a new page
	NewPage(txn transaction.Transaction) (page *pages.RawPage, err error)

	// FreePage deletes a page from the buffer pool. Returns error if the page exists but could not be deleted and
	// panics if page does not exist
	FreePage(txn transaction.Transaction, pageId uint64) error

	// EmptyFrameSize returns the number empty frames which does not hold data of any physical page
	EmptyFrameSize() int
}

type frame struct {
	page  *pages.RawPage
	freed bool
}

var _ IBufferPool = &BufferPool{}

type BufferPool struct {
	poolSize    int
	frames      []frame
	pageMap     map[uint64]int // physical page_id => frame index which keeps that page
	emptyFrames []int          // list of indexes that points to empty frames in the pool
	Replacer    IReplacer
	DiskManager disk.IDiskManager
	lock        sync.Mutex
	logManager  *wal.LogManager
}

func (b *BufferPool) FreePage(txn transaction.Transaction, pageId uint64) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	if frame, ok := b.pageMap[pageId]; ok {
		frame := b.frames[frame]
		// this check might not be necessary, maybe assert and panic since this will be an internal call
		if frame.page.PinCount > 1 {
			return fmt.Errorf("freeing a pinned page, pin count: %v", frame.page.PinCount)
		}

		frame.freed = true
	}

	b.DiskManager.FreePage(pageId)

	b.logManager.AppendLog(wal.NewFreePageLogRecord(txn.GetID(), pageId))
	return nil
}

func (b *BufferPool) GetPage(pageId uint64) (*pages.RawPage, error) {
	b.lock.Lock()
	defer b.lock.Unlock()

	if frameId, ok := b.pageMap[pageId]; ok {
		b.pin(pageId)
		return b.frames[frameId].page, nil
	}

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if len(b.emptyFrames) > 0 {
		// read page and put it inside the frame
		pageData, err := b.DiskManager.ReadPage(pageId)
		if err != nil {
			return nil, err
		}

		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		p := pages.NewRawPage(pageId)
		p.Data = pageData

		b.pageMap[pageId] = emptyFrameIdx
		b.frames[emptyFrameIdx] = frame{page: p}
		b.pin(pageId)
		return p, nil
	}

	// else choose a victim. write victim to disk if it is dirty. read new page and pin it.
	victimFrameIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	pageData, err := b.DiskManager.ReadPage(pageId)
	if err != nil {
		return nil, err
	}

	p := pages.NewRawPage(pageId)
	p.Data = pageData

	b.pageMap[pageId] = victimFrameIdx
	b.frames[victimFrameIdx] = frame{page: p}
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

func (b *BufferPool) Flush(pageId uint64) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		return errors.New("pageId not found")
	}

	frame := b.frames[frameIdx]
	frame.page.WLatch()
	if err := b.DiskManager.WritePage(frame.page.GetData(), frame.page.GetPageId()); err != nil {
		return err
	}
	frame.page.WUnlatch()
	return nil
}

func (b *BufferPool) FlushAll() error {
	// TODO: this implementation is not correct.
	// TODO does this require lock? maybe not since flush acquires lock but empty frames could change in midway flushing
	if err := b.logManager.Flush(); err != nil {
		return err
	}

	dirtyFrames := make([]frame, 0)
	for i, frame := range b.frames {
		flag := 0
		for _, emptyIdx := range b.emptyFrames {
			if i == emptyIdx {
				flag = 1
				break
			}
		}
		if flag == 0 && frame.page.IsDirty() {
			dirtyFrames = append(dirtyFrames, frame)
		}
	}

	sort.Slice(dirtyFrames, func(i, j int) bool {
		return dirtyFrames[i].page.GetPageId() < dirtyFrames[j].page.GetPageId()
	})

	for _, frame := range b.frames {
		if frame.page != nil {
			if err := b.Flush(frame.page.GetPageId()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *BufferPool) NewPage(txn transaction.Transaction) (page *pages.RawPage, err error) {
	// TODO: analyse for resource leaks during rollbacks
	b.lock.Lock()
	defer b.lock.Unlock()

	newPageId := b.DiskManager.NewPage()

	// new page might be popped from free list. in that case it might already be in a frame marked as freed.
	if f, ok := b.pageMap[newPageId]; ok {
		b.pin(newPageId)
		b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), newPageId))
		if b.frames[f].freed != false {
			panic("new page was in pool but freed flag is not set")
		}

		b.frames[f].freed = false
		return b.frames[f].page, nil
	}

	// if there is any empty frame, put new page to there.
	if len(b.emptyFrames) > 0 {
		log.Println("page will be read in an empty frame")
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		p := pages.NewRawPage(newPageId)

		b.pageMap[newPageId] = emptyFrameIdx
		b.frames[emptyFrameIdx] = frame{page: p}
		b.pin(newPageId)
		b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), p.GetPageId()))
		return p, nil
	}

	// if there is no empty frame, evict one and put new page to there.
	victimIdx, err := b.evictVictim()
	if err != nil {
		return nil, err
	}

	p := pages.NewRawPage(newPageId)
	b.pageMap[newPageId] = victimIdx
	b.frames[victimIdx] = frame{page: p}
	b.pin(newPageId)

	b.logManager.AppendLog(wal.NewAllocPageLogRecord(txn.GetID(), p.GetPageId()))
	return p, nil
}

func (b *BufferPool) EmptyFrameSize() int {
	return len(b.emptyFrames)
}

// evictVictim chooses a victim page, writes its data to disk if it is dirty and returns emptied frame's index.
func (b *BufferPool) evictVictim() (int, error) {
	victimFrameIdx, err := b.Replacer.ChooseVictim()
	if err != nil {
		return 0, err
	}

	log.Printf("victim is chosen %v\n", victimFrameIdx)
	victim := b.frames[victimFrameIdx]
	if victim.page.GetPinCount() != 0 {
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.page.GetPinCount(), victim.page.GetPageId()))
	}

	victimPageId := victim.page.GetPageId()
	if victim.page.IsDirty() {
		data := victim.page.GetData()
		if err := b.DiskManager.WritePage(data, victimPageId); err != nil {
			// TODO: victim should be added to replacer as unpinned again since now it will be impossible to choose it
			// as victim again and it is a resource leak.
			log.Print("TODO: resource leak occurred")
			return 0, err
		}
	}

	delete(b.pageMap, victimPageId)
	b.frames[victimFrameIdx].page = nil

	return victimFrameIdx, nil
}

func NewBufferPool(dbFile string, poolSize int) *BufferPool {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}
	d, _, err := disk.NewDiskManager(dbFile)
	common.PanicIfErr(err)
	return &BufferPool{
		poolSize:    poolSize,
		frames:      make([]frame, poolSize),
		pageMap:     map[uint64]int{},
		emptyFrames: emptyFrames,
		Replacer:    NewClockReplacer(poolSize),
		DiskManager: d,
		lock:        sync.Mutex{},
		logManager:  wal.NewLogManager(io.Discard),
	}
}

func NewBufferPoolWithDM(poolSize int, dm disk.IDiskManager, logManager *wal.LogManager) *BufferPool {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}

	if logManager == nil {
		logManager = wal.NewLogManager(io.Discard)
	}

	return &BufferPool{
		poolSize:    poolSize,
		frames:      make([]frame, poolSize),
		pageMap:     map[uint64]int{},
		emptyFrames: emptyFrames,
		Replacer:    NewClockReplacer(poolSize),
		DiskManager: dm,
		lock:        sync.Mutex{},
		logManager:  logManager,
	}
}
