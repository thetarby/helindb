package buffer

import (
	"errors"
	"sync"
)

type LruReplacer struct {
	unpinned []int
	pinned   map[int]int
	size     int
	lock     sync.Mutex
}

func (l *LruReplacer) NumPinnedPages() int {
	return len(l.pinned)
}

var Victims = make(map[int]int)
var Accessed = make(map[int]int)

func (l *LruReplacer) Pin(frameId int) {
	l.lock.Lock()
	defer l.lock.Unlock()

	idx, ok := l.findFrameId(frameId)
	if !ok {
		l.pinned[frameId] = 1
		return
	}
	if len(l.unpinned) == 1 {
		l.unpinned = make([]int, 0)
		l.pinned[frameId] = 1
		return
	}

	copy(l.unpinned[idx:], l.unpinned[idx+1:])
	l.unpinned = l.unpinned[:len(l.unpinned)-1]
	l.pinned[frameId] = 1
}

func (l *LruReplacer) Unpin(frameId int) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if _, ok := l.pinned[frameId]; !ok {
		panic("unpinning a page which is not pinned")
	}

	_, ok := l.findFrameId(frameId)
	if !ok {
		l.unpinned = append(l.unpinned, frameId)
		delete(l.pinned, frameId)
		return
	}
	panic("unpinning a frame which is already unpinned")
}

func (l *LruReplacer) ChooseVictim() (frameId int, err error) {
	if len(l.unpinned) == 0 {
		return 0, errors.New("nothing is unpinned")
	}

	victim := l.unpinned[0]
	val, ok := Victims[victim]
	if !ok {
		Victims[victim] = 1
	} else {
		Victims[victim] = val + 1
	}
	l.unpinned = l.unpinned[1:]
	return victim, nil
}

func (l *LruReplacer) GetSize() int {
	return l.size
}

func (l *LruReplacer) findFrameId(frameId int) (int, bool) {
	for idx, curr := range l.unpinned {
		if curr == frameId {
			return idx, true
		}
	}
	return 0, false
}

func NewLruReplacer(poolSize int) *LruReplacer {
	return &LruReplacer{
		unpinned: make([]int, 0),
		pinned:   make(map[int]int),
		size:     poolSize, // TODO: is size really needed?
		lock:     sync.Mutex{},
	}
}

type IntHeap []int

func (h IntHeap) Len() int {
	return len(h)
}

func (h IntHeap) Less(i, j int) bool {
	return h[i] < h[j]
}

func (h IntHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *IntHeap) Push(x interface{}) {
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
