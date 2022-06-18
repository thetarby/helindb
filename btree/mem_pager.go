package btree

/*
	MemPage and MemPager are in memory implementations of NodePage and Pager defined in persistence.go
*/

import (
	"bytes"
	"encoding/binary"
	"sync"
)

type MemPage struct {
	pageId  Pointer
	data    []byte
	rwLatch *sync.RWMutex
}

func NewMemoryPage(pageId Pointer) *MemPage {
	return &MemPage{
		pageId:  pageId,
		data:    make([]byte, 4096, 4096),
		rwLatch: &sync.RWMutex{},
	}
}

func (n *MemPage) GetData() []byte {
	return n.data
}

func (n *MemPage) GetPageId() Pointer {
	return n.pageId
}

func (n *MemPage) WLatch() {
	n.rwLatch.Lock()
}

func (n *MemPage) WUnlatch() {
	n.rwLatch.Unlock()
}

func (n *MemPage) RLatch() {
	n.rwLatch.RLock()
}

func (n *MemPage) RUnLatch() {
	n.rwLatch.RUnlock()
}

// These will be used by noop peristent pager.
var memPagerLastPageID Pointer = 0
var memPagerNodeMapping = make(map[Pointer]Node)

type MemPager struct {
	lock            *sync.Mutex
	KeySerializer   KeySerializer
	ValueSerializer ValueSerializer
}

func (memPager *MemPager) UnpinByPointer(p Pointer, isDirty bool) {}

func (memPager *MemPager) Unpin(n Node, isDirty bool) {}

func (memPager *MemPager) NewInternalNode(firstPointer Pointer) Node {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	// create a new node
	// TODO: should use an adam akıllı pager
	memPager.lock.Lock()
	memPagerLastPageID++
	newID := memPagerLastPageID
	memPager.lock.Unlock()
	node := PersistentInternalNode{NodePage: NewMemoryPage(newID), pager: memPager, keySerializer: memPager.KeySerializer}
	node.WLatch()
	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	// write first pointer
	buf := bytes.Buffer{}
	err := binary.Write(&buf, binary.BigEndian, firstPointer)
	CheckErr(err)
	asByte := buf.Bytes()
	copy(data[PersistentNodeHeaderSize:], asByte)
	
	memPager.lock.Lock()
	memPagerNodeMapping[newID] = &node
	memPager.lock.Unlock()
	return &node
}

func (memPager *MemPager) NewLeafNode() Node {
	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	// create a new node
	// TODO: should use an adam akıllı pager
	memPager.lock.Lock()
	memPagerLastPageID++
	newID := memPagerLastPageID
	memPager.lock.Unlock()
	var node PersistentLeafNode
	if memPager.ValueSerializer == nil {
		node = PersistentLeafNode{NodePage: NewMemoryPage(newID), pager: memPager, keySerializer: memPager.KeySerializer, valSerializer: &SlotPointerValueSerializer{}}
	} else {
		node = PersistentLeafNode{NodePage: NewMemoryPage(newID), pager: memPager, keySerializer: memPager.KeySerializer, valSerializer: memPager.ValueSerializer}
	}
	node.WLatch()

	// write header
	data := node.GetData()
	WritePersistentNodeHeader(&h, data)

	memPager.lock.Lock()
	memPagerNodeMapping[newID] = &node
	memPager.lock.Unlock()
	return &node
}

func (memPager *MemPager) GetNode(p Pointer, mode TraverseMode) Node {
	memPager.lock.Lock()
	node := memPagerNodeMapping[p]
	memPager.lock.Unlock()
	if node == nil{
		print("")
	}
	if mode == Read {
		node.RLatch()
	} else {
		node.WLatch()
	}
	return node
}

func NewMemPager(serializer KeySerializer, valSerializer ValueSerializer) *MemPager {
	return &MemPager{
		lock:            &sync.Mutex{},
		KeySerializer:   serializer,
		ValueSerializer: valSerializer,
	}
}
