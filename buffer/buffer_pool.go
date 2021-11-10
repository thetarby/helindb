package buffer

import (
	"errors"
	"fmt"
	"helin/disk"
	"log"
	"sync"
)

type IBufferPool interface {
	GetPage(pageId int) (*disk.IPage, error)
	Pin(pageId int) error
	Unpin(pageId int, isDirty bool) bool
	Flush(pageId int) error
	FlushAll() error
	NewPage() (page disk.IPage, err error)
}

type BufferPool struct {
	poolSize    int
	frames      []disk.IPage
	pageMap     map[int]int // physical page_id => frame index which keeps that page
	emptyFrames []int       // list of indexes that points to empty frames in the pool
	replacer    IReplacer
	diskManager disk.IDiskManager
	lock        sync.Mutex
}

const PoolSize = 32

func NewBufferPool(dbFile string, poolsize int) *BufferPool {
	emptyFrames := make([]int, PoolSize, PoolSize)
	for i := 0; i < PoolSize; i++ {
		emptyFrames[i] = i
	}
	d, _ := disk.NewDiskManager(dbFile)
	return &BufferPool{
		poolSize:    poolsize,
		frames:      make([]disk.IPage, PoolSize, PoolSize),
		pageMap:     map[int]int{},
		emptyFrames: emptyFrames,
		diskManager: d,
		lock:        sync.Mutex{},
		replacer:    NewRandomReplacer(),
	}
}

func (b *BufferPool) GetPage(pageId int) (disk.IPage, error) {
	// if page is already in a frame pin and return it
	frameId, ok := b.pageMap[pageId]
	if ok {
		log.Println("page is already in pool, no io required")
		b.pin(pageId)
		return b.frames[frameId], nil
	}

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if len(b.emptyFrames) > 0 {
		log.Println("page will be read in an empty frame")
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		// read page and put it inside the frame
		pageData, err := b.diskManager.ReadPage(pageId)
		if err != nil {
			return nil, err
		}

		p := disk.NewRawPage(pageId)
		p.Data = pageData

		b.pageMap[pageId] = emptyFrameIdx
		b.frames[emptyFrameIdx] = p
		b.pin(pageId)
		return p, nil
	}

	// else choose a victim. write victim to disk if it is dirty. read new page and pin it.
	victimIdx, err := b.replacer.ChooseVictim()
	log.Printf("victim is chosen %v\n", victimIdx)
	if err != nil {
		return nil, err
	}

	victim := b.frames[victimIdx]
	if victim.GetPinCount() != 0 {
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.GetPinCount(), victim.GetPageId()))
	}

	victimPageId := victim.GetPageId()
	if victim.IsDirty() {
		data := victim.GetData()
		b.diskManager.WritePage(data, victimPageId)
	}

	pageData, err := b.diskManager.ReadPage(pageId)
	if err != nil {
		return nil, err
	}

	p := disk.NewRawPage(pageId)
	p.Data = pageData

	b.pageMap[pageId] = victimIdx
	delete(b.pageMap, victimPageId)
	b.frames[victimIdx] = p
	b.pin(pageId)
	return p, err
}

// pin increments page's pin count and pins the frame that keeps the page to avoid it being chosen as victim
func (b *BufferPool) pin(pageId int) error {
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		panic(fmt.Sprintf("pinned a page which does not exist: %v", pageId))
	}

	page := b.frames[frameIdx]
	page.IncrPinCount()
	b.replacer.Pin(frameIdx)
	return nil
}

func (b *BufferPool) Unpin(pageId int, isDirty bool) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		panic(fmt.Sprintf("unpinned a page which does not exist: %v", pageId))
	}

	// if found set its dirty field
	page := b.frames[frameIdx]
	if isDirty {
		page.SetDirty()
	}

	// if pin count is already 0 it is already unpinned. Although that should not happen I guess
	if page.GetPinCount() == 0 {
		log.Printf("buffer.Unpin is called while pin count is already zero. PageId: %v\n", pageId)
		return true
	}

	// decrease pin count and if it is 0 unpin frame in the replacer so that new pages can be read
	page.DecrPinCount()
	if page.GetPinCount() == 0 {
		b.replacer.Unpin(frameIdx)
		return true
	}
	return false
}

func (b *BufferPool) Flush(pageId int) error {
	b.lock.Lock()
	defer b.lock.Unlock()
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		return errors.New("pageId not found")
	}

	page := b.frames[frameIdx]
	page.WLatch()
	if err := b.diskManager.WritePage(page.GetData(), page.GetPageId()); err != nil {
		return err
	}
	page.SetClean()
	page.WUnlatch()
	return nil
}

func (b *BufferPool) FlushAll() error {
	// TODO does this require lock? maybe not since flush acquires lock but empty frames could change in midway flushing
	for i, frame := range b.frames {
		flag := 0
		for _, emptyIdx := range b.emptyFrames {
			if i == emptyIdx {
				flag = 1
				break
			}
		}
		if flag == 0 && frame.IsDirty() {
			if err := b.Flush(frame.GetPageId()); err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *BufferPool) NewPage() (page disk.IPage, err error) {
	// TODO: too many duplicate code with GetPage
	b.lock.Lock()
	defer b.lock.Unlock()

	newPageId := b.diskManager.NewPage()
	if len(b.emptyFrames) > 0 {
		log.Println("page will be read in an empty frame")
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		p := disk.NewRawPage(newPageId)

		b.pageMap[newPageId] = emptyFrameIdx
		b.frames[emptyFrameIdx] = p
		b.pin(newPageId)
		return p, nil
	}

	victimIdx, err := b.replacer.ChooseVictim()
	log.Printf("victim is chosen %v\n", victimIdx)
	if err != nil {
		return nil, err
	}

	victim := b.frames[victimIdx]
	if victim.GetPinCount() != 0 {
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.GetPinCount(), victim.GetPageId()))
	}

	victimPageId := victim.GetPageId()
	if victim.IsDirty() {
		data := victim.GetData()
		b.diskManager.WritePage(data, victimPageId)
	}

	p := disk.NewRawPage(newPageId)
	b.pageMap[newPageId] = victimIdx
	delete(b.pageMap, victimPageId)
	b.frames[victimIdx] = p
	b.pin(newPageId)
	return p, err
}
