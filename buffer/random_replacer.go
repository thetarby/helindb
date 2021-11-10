package buffer

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

type RandomReplacer struct {
	pinned map[int]int
	size   int
	lock   sync.Mutex
}

func NewRandomReplacer(poolSize int) *RandomReplacer {
	return &RandomReplacer{
		pinned: make(map[int]int),
		size:   poolSize,
	}
}

func (r *RandomReplacer) Pin(frameId int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.pinned[frameId] = 1
}

func (r *RandomReplacer) Unpin(frameId int) {
	r.lock.Lock()
	defer r.lock.Unlock()

	delete(r.pinned, frameId)
}

func (r *RandomReplacer) ChooseVictim() (frameId int, err error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	frames := make([]int, r.size, r.size)
	for i := 0; i < len(frames); i++ {
		frames[i] = i
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(frames), func(i, j int) { frames[i], frames[j] = frames[j], frames[i] })

	for _, frameIdx := range frames {
		if _, ok := r.pinned[frameIdx]; ok {
			continue
		}
		return frameIdx, nil
	}

	return 0, errors.New("all frames are unpinned")
}

func (r *RandomReplacer) GetSize() int {
	// NOTE: no need thread safe access
	return r.size
}
