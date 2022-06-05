package buffer

import (
	"errors"
	"fmt"
	"helin/disk"
	"helin/disk/pages"
	"log"
	"sort"
	"sync"
)

type IBufferPool interface {
	GetPage(pageId int) (*pages.RawPage, error)
	pin(pageId int) error
	Unpin(pageId int, isDirty bool) bool
	Flush(pageId int) error
	FlushAll() error
	NewPage() (page *pages.RawPage, err error)
	EmptyFrameSize() int
}

type BufferPool struct {
	poolSize    int
	frames      []*pages.RawPage
	pageMap     map[int]int // physical page_id => frame index which keeps that page
	emptyFrames []int       // list of indexes that points to empty frames in the pool
	Replacer    IReplacer
	DiskManager disk.IDiskManager
	lock        sync.Mutex
}

func NewBufferPool(dbFile string, poolSize int) *BufferPool {
	emptyFrames := make([]int, poolSize)
	for i := 0; i < poolSize; i++ {
		emptyFrames[i] = i
	}
	d, _ := disk.NewDiskManager(dbFile)
	return &BufferPool{
		poolSize:    poolSize,
		frames:      make([]*pages.RawPage, poolSize),
		pageMap:     map[int]int{},
		emptyFrames: emptyFrames,
		DiskManager: d,
		lock:        sync.Mutex{},
		Replacer:    NewLruReplacer(poolSize),
	}
}

func (b *BufferPool) GetPage(pageId int) (*pages.RawPage, error) {
	// if page is already in a frame pin and return it
	//val, ok := Accessed[pageId]
	//if !ok{
	//	Accessed[pageId]=1
	//}else{
	//	Accessed[pageId] = val+1
	//}
	//fmt.Println(b.pageMap)
	b.lock.Lock()
	defer b.lock.Unlock()

	if frameId, ok := b.pageMap[pageId]; ok {
		b.pin(pageId)
		return b.frames[frameId], nil
	}

	// if page not found in frames and there is an empty frame. read page to frame and pin it.
	if len(b.emptyFrames) > 0 { // TODO: these are not thread safe. make buffer pool is not thread safe
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
		b.frames[emptyFrameIdx] = p
		b.pin(pageId)
		return p, nil
	}

	// else choose a victim. write victim to disk if it is dirty. read new page and pin it.
	victimFrameIdx, err := b.Replacer.ChooseVictim()
	if err != nil {
		return nil, err
	}
	log.Printf("victim is chosen %v\n", victimFrameIdx)

	victim := b.frames[victimFrameIdx]
	if victim.GetPinCount() != 0 {
		panic(fmt.Sprintf("a page is chosen as victim while it's pin count is not zero. pin count: %v, page_id: %v", victim.GetPinCount(), victim.GetPageId()))
	}

	victimPageId := victim.GetPageId()
	if victim.IsDirty() {
		data := victim.GetData()
		if err := b.DiskManager.WritePage(data, victimPageId); err != nil {
			// TODO: victim should be added to replacer as unpinned again since now it will be impossible to choose it 
			// as victim again and it is a resource leak.
			log.Print("TODO: resource leak occurred")
			return nil, err
		}
	}

	pageData, err := b.DiskManager.ReadPage(pageId)
	if err != nil {
		// TODO: victim should be added to replacer as unpinned again since now it will be impossible to choose it 
		// as victim again and it is a resource leak.
		log.Print("TODO: resource leak occurred")
		return nil, err
	}

	p := pages.NewRawPage(pageId)
	p.Data = pageData

	b.pageMap[pageId] = victimFrameIdx
	delete(b.pageMap, victimPageId)
	b.frames[victimFrameIdx] = p
	b.pin(pageId)
	return p, err
}

// pin increments page's pin count and pins the frame that keeps the page to avoid it being chosen as victim
func (b *BufferPool) pin(pageId int) {
	frameIdx, ok := b.pageMap[pageId]
	if !ok {
		// TODO: is panic ok here? this method is private and should not be called with a non existent
		// pageID hence panic might be ok?
		panic(fmt.Sprintf("pinned a page which does not exist: %v", pageId))
	}

	page := b.frames[frameIdx]
	page.IncrPinCount()
	b.Replacer.Pin(frameIdx)
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
		panic(fmt.Sprintf("buffer.Unpin is called while pin count is already zero. PageId: %v\n", pageId))
	}

	// decrease pin count and if it is 0 unpin frame in the replacer so that new pages can be read
	page.DecrPinCount()
	if page.GetPinCount() == 0 {
		b.Replacer.Unpin(frameIdx)
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
	if err := b.DiskManager.WritePage(page.GetData(), page.GetPageId()); err != nil {
		return err
	}
	page.SetClean() // TODO: should this happen ?
	page.WUnlatch()
	return nil
}

func (b *BufferPool) FlushAll() error {
	// TODO does this require lock? maybe not since flush acquires lock but empty frames could change in midway flushing
	dirtyFrames := make([]*pages.RawPage, 0)
	for i, frame := range b.frames {
		flag := 0
		for _, emptyIdx := range b.emptyFrames {
			if i == emptyIdx {
				flag = 1
				break
			}
		}
		if flag == 0 { //&& frame.IsDirty() { // TODO: do not forget to uncomment this
			dirtyFrames = append(dirtyFrames, frame)
		}
	}

	sort.Slice(dirtyFrames, func(i, j int) bool {
		return dirtyFrames[i].GetPageId() < dirtyFrames[j].GetPageId()
	})

	for _, frame := range dirtyFrames {
		if err := b.Flush(frame.GetPageId()); err != nil {
			return err
		}
	}
	return nil
}

func (b *BufferPool) NewPage() (page *pages.RawPage, err error) {
	// TODO: too many duplicate code with GetPage
	b.lock.Lock()
	defer b.lock.Unlock()

	newPageId := b.DiskManager.NewPage()
	if len(b.emptyFrames) > 0 {
		log.Println("page will be read in an empty frame")
		emptyFrameIdx := b.emptyFrames[0]
		b.emptyFrames = b.emptyFrames[1:]

		p := pages.NewRawPage(newPageId)

		b.pageMap[newPageId] = emptyFrameIdx
		b.frames[emptyFrameIdx] = p
		b.pin(newPageId)
		return p, nil
	}

	victimIdx, err := b.Replacer.ChooseVictim()
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
		b.DiskManager.WritePage(data, victimPageId)
	}

	p := pages.NewRawPage(newPageId)
	b.pageMap[newPageId] = victimIdx
	delete(b.pageMap, victimPageId)
	b.frames[victimIdx] = p
	b.pin(newPageId)
	return p, err
}

func (b *BufferPool) EmptyFrameSize() int {
	return len(b.emptyFrames)
}
