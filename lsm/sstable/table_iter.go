package sstable

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
)

type tableIter struct {
	readerAt io.ReaderAt
	cmp      Comparer
	err      error

	// currDataBlockIt changes when current data block is finished and next block is read from index block
	currDataBlockIt *blockIter

	// there is only one indexBlock in each table hence this is constant
	// and keeps offets of each data block in the table. In an index block keys are
	// separators between blocks and values are block handles. There is one entry for each
	// data block in the index block.
	indexBlockIt *blockIter
}

func (i *tableIter) Next() bool {
	if i.currDataBlockIt == nil {
		return false
	}

	for {
		// first iterate over current data block
		if i.currDataBlockIt.Next() {
			return true
		}
		if i.currDataBlockIt.err != nil {
			i.err = i.currDataBlockIt.err
			break
		}

		// if it is finished read next data block from index block
		if !i.nextBlock(nil) {
			break
		}
	}
	return false
}

func (i *tableIter) Key() []byte {
	if i.currDataBlockIt == nil {
		return nil
	}
	return i.currDataBlockIt.Key()
}

func (i *tableIter) Value() []byte {
	if i.currDataBlockIt == nil {
		return nil
	}
	return i.currDataBlockIt.Value()
}

func (i *tableIter) Close() error {
	i.currDataBlockIt = nil
	return i.err
}

// nextBlock reads next entry from index block and sets current data block by decoding block handle.
func (i *tableIter) nextBlock(key []byte) bool {
	if !i.indexBlockIt.Next() {
		i.err = i.indexBlockIt.err
		return false
	}

	// read next value in the index, which is a block handle
	v := i.indexBlockIt.Value()
	h, n := decodeBlockHandle(v)
	if n == 0 || n != len(v) {
		i.err = errors.New("leveldb/table: corrupt index entry")
		return false
	}

	// read actual block
	k, err := i.readBlock(h)
	if err != nil {
		i.err = err
		return false
	}

	// seek key in the block
	data, err := k.seek(i.cmp, key)
	if err != nil {
		i.err = err
		return false
	}

	i.currDataBlockIt = data
	return true
}

// readBlock returns a block given a block handle.
func (r *tableIter) readBlock(bh blockHandle) (block, error) {
	b := make([]byte, bh.length)
	n, err := r.readerAt.ReadAt(b, int64(bh.offset))
	if err != nil {
		return nil, errors.Wrap(err, "error while reading at offset")
	}
	if n != int(bh.length) {
		return nil, fmt.Errorf("block handle is not valid. bh.length is %v, read length is %v", bh.length, n)
	}

	return b, nil
}

func NewTableIter(key []byte, reader io.ReaderAt, idxBlock block, cmp Comparer) *tableIter {
	indexIt, err := idxBlock.seek(cmp, key)
	if err != nil {
		return &tableIter{err: err}
	}

	i := &tableIter{
		readerAt:     reader,
		cmp:          cmp,
		indexBlockIt: indexIt,
	}
	i.nextBlock(key)

	return i
}
