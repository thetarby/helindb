package btree

/*
	MemPager is in memory implementations of Pager defined in persistence.go
*/

import (
	"helin/disk/pages"
	"helin/disk/wal"
	"helin/transaction"
	"sync"
)

// These will be used by noop persistent pager.
var memPagerLastPageID Pointer = 0
var memPagerNodeMapping = make(map[Pointer]Node)
var memPagerNodeMapping2 = make(map[Pointer]NodePage)

var _ Pager = &MemPager{}

type MemPager struct {
	lock            *sync.Mutex
	KeySerializer   KeySerializer
	ValueSerializer ValueSerializer
	LogManager      wal.LogManager
}

// Free implements Pager
func (memPager *MemPager) Free(txn transaction.Transaction, p Pointer) error {
	memPager.lock.Lock()
	defer memPager.lock.Unlock()

	delete(memPagerNodeMapping, p)
	delete(memPagerNodeMapping2, p)
	return nil
}

// FreeNode implements Pager
func (memPager *MemPager) FreeNode(txn transaction.Transaction, n Node) {
	memPager.lock.Lock()
	defer memPager.lock.Unlock()
	delete(memPagerNodeMapping, n.GetPageId())
	delete(memPagerNodeMapping2, n.GetPageId())
	return
}

func (memPager *MemPager) CreatePage(transaction.Transaction) NodePage {
	memPager.lock.Lock()
	defer memPager.lock.Unlock()
	memPagerLastPageID++
	newID := memPagerLastPageID
	sp := InitLoggedSlottedPage(pages.NewRawPage(uint64(newID)), memPager.LogManager)
	memPagerNodeMapping2[newID] = &sp
	return &sp
}

func (memPager *MemPager) GetPage(p Pointer) NodePage {
	memPager.lock.Lock()
	node := memPagerNodeMapping2[p]
	memPager.lock.Unlock()

	return node
}

func (memPager *MemPager) UnpinByPointer(p Pointer, isDirty bool) {}

func (memPager *MemPager) Unpin(n Node, isDirty bool) {}

func (memPager *MemPager) NewInternalNode(txn transaction.Transaction, firstPointer Pointer) NodeReleaser {
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
		p:             InitLoggedSlottedPage(pages.NewRawPage(uint64(newID)), memPager.LogManager),
		keySerializer: memPager.KeySerializer,
		pager:         memPager,
	}
	node.WLatch()
	// set header
	node.SetHeader(txn, &h)

	// write first pointer
	node.setValueAt(txn, 0, firstPointer)

	memPager.lock.Lock()
	memPagerNodeMapping[newID] = &node
	memPager.lock.Unlock()
	return &memNodeWriteReleaser{&node}
}

func (memPager *MemPager) NewLeafNode(txn transaction.Transaction) NodeReleaser {
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
		p:             InitLoggedSlottedPage(pages.NewRawPage(uint64(newID)), memPager.LogManager),
		keySerializer: memPager.KeySerializer,
		valSerializer: memPager.ValueSerializer,
		pager:         memPager,
	}
	node.WLatch()

	// write header
	node.SetHeader(txn, &h)
	memPager.lock.Lock()
	memPagerNodeMapping[newID] = &node
	memPager.lock.Unlock()
	return &memNodeWriteReleaser{&node}
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

func (memPager *MemPager) GetNodeReleaser(p Pointer, mode TraverseMode) NodeReleaser {
	if mode == Read {
		return &memNodeReadReleaser{memPager.GetNode(p, mode)}
	}
	return &memNodeWriteReleaser{memPager.GetNode(p, mode)}
}

func NewMemPager(serializer KeySerializer, valSerializer ValueSerializer) *MemPager {
	return &MemPager{
		lock:            &sync.Mutex{},
		KeySerializer:   serializer,
		ValueSerializer: valSerializer,
		LogManager:      wal.NoopLM,
	}
}

type memNodeReadReleaser struct {
	Node
}

func (n *memNodeReadReleaser) Release() {
	n.RUnLatch()
}

type memNodeWriteReleaser struct {
	Node
}

func (n *memNodeWriteReleaser) Release() {
	n.WUnlatch()
}
