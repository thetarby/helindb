package buffer

import (
	"container/heap"
	"sync"
	"time"
)

type LruReplacer struct {
	pinned  map[int]int
	minHeap *IntHeap
	size    int
	lock    sync.Mutex
}

func (l *LruReplacer) Pin(frameId int) {
	l.lock.Lock()
	defer l.lock.Unlock()

	now := int(time.Now().UnixNano())
	heap.Push(l.minHeap, now)
	l.pinned[frameId] = 1
}

func (l *LruReplacer) Unpin(frameId int) {
	l.lock.Lock()
	defer l.lock.Unlock()

	delete(l.pinned, frameId)
}

func (l *LruReplacer) ChooseVictim() (frameId int, err error) {
	panic("implement me")
}

func (l *LruReplacer) GetSize() int {
	return l.size
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
