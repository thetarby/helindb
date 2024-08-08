package btree

import (
	"helin/common"
	"helin/transaction"
)

// Pager2 wraps BPager and encapsulates node management methods for a BTree.
type Pager2 struct {
	bpager BPager
	ks     KeySerializer
	vs     ValueSerializer
}

func NewPager2(bpager BPager, ks KeySerializer, vs ValueSerializer) *Pager2 {
	return &Pager2{bpager: bpager, ks: ks, vs: vs}
}

func (p2 *Pager2) NewInternalNode(txn transaction.Transaction, firstPointer Pointer) NodeReleaser {
	bpage := p2.bpager.NewBPage(txn)
	//bpage.WLatch()

	h := PersistentNodeHeader{
		IsLeaf: 0,
		KeyLen: 0,
	}

	node := VarKeyInternalNode{
		p:             bpage,
		keySerializer: p2.ks,
		pager:         p2,
	}
	// set header
	node.SetHeader(txn, &h)

	// write first pointer
	node.SetValueAt(txn, 0, firstPointer)

	return &writeNodeReleaser2{
		node:   &node,
		bpager: p2.bpager,
	}
}

func (p2 *Pager2) NewLeafNode(txn transaction.Transaction) NodeReleaser {
	bpage := p2.bpager.NewBPage(txn)
	//bpage.WLatch()

	h := PersistentNodeHeader{
		IsLeaf: 1,
		KeyLen: 0,
	}

	node := VarKeyLeafNode{
		p:             bpage,
		keySerializer: p2.ks,
		valSerializer: p2.vs,
		pager:         p2,
	}

	// write header
	node.SetHeader(txn, &h)

	return &writeNodeReleaser2{
		node:   &node,
		bpager: p2.bpager,
	}
}

func (p2 *Pager2) GetNodeReleaser(p Pointer, mode TraverseMode) NodeReleaser {
	n := p2.getNode(p, mode)
	if n == nil {
		return nil
	}
	if mode == Read {
		return &readNodeReleaser2{
			node:   n,
			bpager: p2.bpager,
		}
	} else {
		return &writeNodeReleaser2{
			node:   n,
			bpager: p2.bpager,
		}
	}
}

func (p2 *Pager2) FreeNode(txn transaction.Transaction, n Pointer) {
	p2.bpager.FreeBPage(txn, n)
}

func (p2 *Pager2) CreatePage(txn transaction.Transaction) BPageReleaser {
	bpage := p2.bpager.NewBPage(txn)
	//bpage.WLatch()

	return &writeBpageReleaser2{
		BPage:  bpage,
		bpager: p2.bpager,
	}
}

func (p2 *Pager2) GetPage(p Pointer, readOnly bool) BPageReleaser {
	bpage, err := p2.bpager.GetBPage(p)
	common.PanicIfErr(err)

	if readOnly {
		bpage.RLatch()
		return &readBpageReleaser2{
			BPage:  bpage,
			bpager: p2.bpager,
		}
	} else {
		bpage.WLatch()
		return &writeBpageReleaser2{
			BPage:  bpage,
			bpager: p2.bpager,
		}
	}
}

func (p2 *Pager2) CreateOverflow(txn transaction.Transaction) OverflowReleaser {
	return p2.bpager.CreateOverflow(txn)
}

func (p2 *Pager2) FreeOverflow(txn transaction.Transaction, p Pointer) {
	p2.bpager.FreeOverflow(txn, p)
}

func (p2 *Pager2) GetOverflowReleaser(p Pointer) OverflowReleaser {
	return p2.bpager.GetOverflowReleaser(p)
}

func (p2 *Pager2) getNode(p Pointer, mode TraverseMode) node {
	if p == 0 {
		return nil
	}

	bpage, err := p2.bpager.GetBPage(p)
	common.PanicIfErr(err)

	if mode == Read {
		bpage.RLatch()
	} else {
		bpage.WLatch()
	}

	h := ReadPersistentNodeHeader(bpage.GetAt(0))
	if h.IsLeaf == 1 {
		return &VarKeyLeafNode{
			p:             bpage,
			keySerializer: p2.ks,
			valSerializer: p2.vs,
			pager:         p2,
		}
	}
	return &VarKeyInternalNode{
		p:             bpage,
		keySerializer: p2.ks,
		pager:         p2,
	}
}

type readNodeReleaser2 struct {
	node
	bpager BPager
}

func (n *readNodeReleaser2) Release() {
	n.bpager.Unpin(n.GetPageId())
	n.RUnLatch()
}

type writeNodeReleaser2 struct {
	node
	bpager BPager
}

func (n *writeNodeReleaser2) Release() {
	n.bpager.Unpin(n.GetPageId())
	n.WUnlatch()
}

type readBpageReleaser2 struct {
	BPage
	bpager BPager
}

func (n *readBpageReleaser2) Release() {
	n.bpager.Unpin(n.GetPageId())
	n.RUnLatch()
}

type writeBpageReleaser2 struct {
	BPage
	bpager BPager
}

func (n *writeBpageReleaser2) Release() {
	n.bpager.Unpin(n.GetPageId())
	n.WUnlatch()
}
