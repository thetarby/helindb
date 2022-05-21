package sstable

// blockIter is an iterator over a single block of data.
type blockIter struct {
	block       block
	blockOffset uint
	key, val    []byte
	err         error

	done, initialized bool
}

func (i *blockIter) Next() bool {
	if i.done || i.err != nil {
		return false
	}
	if !i.initialized{
		return true
	}
	if len(i.block) == int(i.blockOffset) {
		i.Close()
		return false
	}

	if i.key == nil {
		i.key = make([]byte, 0, 256)
	}

	n, sharedLen, _, _, nonSharedKey, val := i.block.readEntry(i.blockOffset)

	i.key = append(i.key[:sharedLen], nonSharedKey...)
	i.val = val
	i.blockOffset += uint(n)
	return true
}

func (i *blockIter) Key() []byte {
	if i.key == nil {
		return nil
	}

	return i.key[:len(i.key):len(i.key)]
}

func (i *blockIter) Value() []byte {
	if i.val == nil {
		return nil
	}

	return i.val[:len(i.val):len(i.val)]
}

func (i *blockIter) Close() error {
	i.key = nil
	i.val = nil
	i.done = true
	return i.err
}

func NewBlockIter(block []byte) *blockIter {
	return &blockIter{
		block:       block,
		blockOffset: 0,
		key:         nil,
		val:         nil,
		err:         nil,
		done:        false,
		initialized: true,
	}
}

func NewBlockIterFrom(block []byte, key []byte, cmp Comparer) (*blockIter, error) {
	bit := NewBlockIter(block)

	// iterate until key is gte
	for bit.Next() && cmp.Compare(bit.key, key) < 0 {}
	if bit.err != nil {
		return nil, bit.err
	}

	bit.initialized = false
	return bit, nil
}
