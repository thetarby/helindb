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

func (p2 *Pager2) NewInternalNode(txn transaction.Transaction, firstPointer Pointer) (nodeReleaser, error) {
	bpage, err := p2.bpager.NewBPage(txn)
	if err != nil {
		return nil, err
	}

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

	return &writeNodeReleaser{
		node: &node,
		p:    bpage,
	}, nil
}

func (p2 *Pager2) NewLeafNode(txn transaction.Transaction) (nodeReleaser, error) {
	bpage, err := p2.bpager.NewBPage(txn)
	if err != nil {
		return nil, err
	}

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

	return &writeNodeReleaser{
		node: &node,
		p:    bpage,
	}, nil
}

func (p2 *Pager2) GetNodeReleaser(txn transaction.Transaction, p Pointer, mode TraverseMode) nodeReleaser {
	n, page := p2.getNode(txn, p, mode)
	if n == nil {
		return nil
	}
	if mode == Read {
		return &readNodeReleaser{
			node: n,
			p:    page,
		}
	} else {
		return &writeNodeReleaser{
			node: n,
			p:    page,
		}
	}
}

func (p2 *Pager2) FreeNode(txn transaction.Transaction, n Pointer) {
	p2.bpager.FreeBPage(txn, n)
}

func (p2 *Pager2) CreatePage(txn transaction.Transaction) (BPageReleaser, error) {
	bpage, err := p2.bpager.NewBPage(txn)
	if err != nil {
		return nil, err
	}

	return bpage, nil
}

func (p2 *Pager2) GetPage(txn transaction.Transaction, p Pointer, readOnly bool) BPageReleaser {
	if readOnly {
		bpage, err := p2.bpager.GetBPageToRead(txn, p)
		common.PanicIfErr(err)

		return bpage
	} else {
		bpage, err := p2.bpager.GetBPageToWrite(txn, p)
		common.PanicIfErr(err)

		return bpage
	}
}

func (p2 *Pager2) CreateOverflow(txn transaction.Transaction) (OverflowReleaser, error) {
	return p2.bpager.CreateOverflow(txn)
}

func (p2 *Pager2) FreeOverflow(txn transaction.Transaction, p Pointer) error {
	return p2.bpager.FreeOverflow(txn, p)
}

func (p2 *Pager2) GetOverflowReleaser(p Pointer) (OverflowReleaser, error) {
	return p2.bpager.GetOverflowReleaser(p)
}

func (p2 *Pager2) getNode(txn transaction.Transaction, p Pointer, mode TraverseMode) (node, BPageReleaser) {
	if p == 0 {
		return nil, nil
	}

	var bpage BPageReleaser
	var err error

	if mode == Read {
		bpage, err = p2.bpager.GetBPageToRead(txn, p)
		common.PanicIfErr(err)
	} else {
		bpage, err = p2.bpager.GetBPageToWrite(txn, p)
		common.PanicIfErr(err)
	}

	h := ReadPersistentNodeHeader(bpage.GetAt(0))
	if h.IsLeaf == 1 {
		return &VarKeyLeafNode{
			p:             bpage,
			keySerializer: p2.ks,
			valSerializer: p2.vs,
			pager:         p2,
		}, bpage
	}
	return &VarKeyInternalNode{
		p:             bpage,
		keySerializer: p2.ks,
		pager:         p2,
	}, bpage
}

type readNodeReleaser struct {
	node
	p BPageReleaser
}

func (n *readNodeReleaser) Release() {
	n.p.Release()
}

type writeNodeReleaser struct {
	node
	p BPageReleaser
}

func (n *writeNodeReleaser) Release() {
	n.p.Release()
}
