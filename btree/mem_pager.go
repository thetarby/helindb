package btree

/*
	MemPager is in memory implementations of Pager defined in persistence.go
*/

import (
	"helin/disk/pages"
	"sync"
)

// These will be used by noop peristent pager.
var memPagerLastPageID Pointer = 0
var memPagerNodeMapping = make(map[Pointer]Node)
var memPagerNodeMapping2 = make(map[Pointer]NodePage)

var _ Pager = &MemPager{}

type MemPager struct {
	lock            *sync.Mutex
	KeySerializer   KeySerializer
	ValueSerializer ValueSerializer
}

// Free implements Pager
func (*MemPager) Free(p Pointer) error {
	delete(memPagerNodeMapping, p)
	delete(memPagerNodeMapping2, p)
	return nil
}

// FreeNode implements Pager
func (*MemPager) FreeNode(n Node) error {
	delete(memPagerNodeMapping, n.GetPageId())
	delete(memPagerNodeMapping2, n.GetPageId())
	return nil
}

func (memPager *MemPager) CreatePage() NodePage {
	memPager.lock.Lock()
	defer memPager.lock.Unlock()
	memPagerLastPageID++
	newID := memPagerLastPageID
	p := &BtreePage{*pages.NewRawPage(int(newID))}
	memPagerNodeMapping2[newID] = p
	return p
}

func (memPager *MemPager) GetPage(p Pointer) NodePage {
	memPager.lock.Lock()
	node := memPagerNodeMapping2[p]
	memPager.lock.Unlock()
	if node == nil {
		print("")
	}

	return node
}

func (memPager *MemPager) UnpinByPointer(p Pointer, isDirty bool) {}

func (memPager *MemPager) Unpin(n Node, isDirty bool) {}

func (memPager *MemPager) NewInternalNode(firstPointer Pointer) Node {
	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	// create a new node
	memPager.lock.Lock()
	memPagerLastPageID++
	newID := memPagerLastPageID
	memPager.lock.Unlock()
	node := VarKeyInternalNode{
		p:             InitSlottedPage(&BtreePage{*pages.NewRawPage(int(newID))}),
		keySerializer: memPager.KeySerializer,
		pager:         memPager,
	}
	node.WLatch()
	// set header
	node.SetHeader(&h)

	// write first pointer
	node.setValueAt(0, firstPointer)

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
	memPager.lock.Lock()
	memPagerLastPageID++
	newID := memPagerLastPageID
	memPager.lock.Unlock()
	node := VarKeyLeafNode{
		p:             InitSlottedPage(&BtreePage{*pages.NewRawPage(int(newID))}),
		keySerializer: memPager.KeySerializer,
		valSerializer: memPager.ValueSerializer,
		pager:         memPager,
	}
	node.WLatch()

	// write header
	node.SetHeader(&h)
	memPager.lock.Lock()
	memPagerNodeMapping[newID] = &node
	memPager.lock.Unlock()
	return &node
}

func (memPager *MemPager) GetNode(p Pointer, mode TraverseMode) Node {
	memPager.lock.Lock()
	node := memPagerNodeMapping[p]
	memPager.lock.Unlock()
	if node == nil {
		panic("node not found")
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
