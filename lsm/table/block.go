package sstable

import (
	"encoding/binary"
	"sort"

	"github.com/pkg/errors"
)

/**
 * block format:
 *  ---------------------------------------------------------
 *  | HEADER | ... Entries ... | ... OffsetsOfRestartPoints(NRP of uint32 values) ...| NumberOfRestartPoints(NRP) |
 *  ---------------------------------------------------------
 *
 *  OffsetsOfRestartPoints and NumberOfRestartPoints combined is called block trailer.
 *
 *  Entry format (size in bytes):
 *  ----------------------------------------------------------------------------
 *  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
 *  ----------------------------------------------------------------------------
 */

type blockHandle struct {
	offset, length uint64
}

func decodeBlockHandle(src []byte) (blockHandle, int) {
	offset, n := binary.Uvarint(src)
	length, m := binary.Uvarint(src[n:])
	if n == 0 || m == 0 {
		return blockHandle{}, 0
	}
	return blockHandle{offset, length}, n + m
}

func encodeBlockHandle(dst []byte, b blockHandle) int {
	n := binary.PutUvarint(dst, b.offset)
	m := binary.PutUvarint(dst[n:], b.length)
	return n + m
}

type block []byte

func (b block) NumRestarts() int{
	return int(binary.LittleEndian.Uint32(b[len(b)-4:]))
}

func (b block) blockTrailerOffset() int{
	return len(b) - 4 * (1 + b.NumRestarts())
}

func (b block) rpAt(idx int) (key []byte, val []byte){
	rpOffset := int(binary.LittleEndian.Uint32(b[b.blockTrailerOffset()+4*idx:]))
	// For a restart point, there are 0 bytes shared with the previous key.
	// The varint encoding of 0 occupies 1 byte.
	rpOffset++
	// Decode the key at that restart point, and compare it to the key sought.
	notSharedLen, n1 := binary.Uvarint(b[rpOffset:])
	valLen, n2 := binary.Uvarint(b[rpOffset+n1:])
	keyBeginning := rpOffset + n1 + n2
	key = b[keyBeginning : keyBeginning+int(notSharedLen)]
	val = b[keyBeginning+int(notSharedLen) : keyBeginning+int(notSharedLen) + int(valLen)]

	return key, val
}

// reads entry at given offset
func (b block) readEntry(offset uint) (n int, sharedLen, nonSharedLen, valLen uint64, nonSharedKey, val []byte){
	data := b[offset:]
	sharedLen, n1 := binary.Uvarint(data)
	nonSharedLen, n2 := binary.Uvarint(data[n1:])
	valLen, n3 := binary.Uvarint(data[n1+n2:])
	nonSharedKey = data[n1+n2+n3:n1+n2+n3+int(nonSharedLen)]
	val = data[n1+n2+n3+int(nonSharedLen):n1+n2+n3+int(nonSharedLen)+int(valLen)]

	return n1+n2+n3+int(nonSharedLen)+int(valLen), sharedLen, nonSharedLen, valLen, nonSharedKey, val
}

// seek returns a blockIter positioned at the first key/value pair whose key is
// >= the given key. If there is no such key, the blockIter returned is done.
func (b block) seek(cmp Comparer, key []byte) (*blockIter, error) {
	numRestarts := b.NumRestarts()
	if numRestarts == 0 {
		return nil, errors.New("leveldb/table: invalid table (block has no restart points)")
	}

	blockTrailerOffset := b.blockTrailerOffset()
	var offset int
	if len(key) > 0 {
		// Find the index of the smallest restart point whose key is > the key
		// sought; index will be numRestarts if there is no such restart point.
		index := sort.Search(numRestarts, func(i int) bool {
			rpKey, _ := b.rpAt(i)
			return cmp.Compare(rpKey, key) > 0
		})

		// Since keys are strictly increasing, if index > 0 then the restart
		// point at index-1 will be the largest whose key is <= the key sought.
		// If index == 0, then all keys in this block are larger than the key
		// sought, and offset remains at zero.
		if index > 0 {
			offset = int(binary.LittleEndian.Uint32(b[blockTrailerOffset+4*(index-1):]))
		}
	}
	// Initialize the blockIter to the restart point.,
	bIt, err := NewBlockIterFrom(b[offset:blockTrailerOffset], key, cmp)
	if err != nil {
		return nil, errors.Wrap(err, "error while creating block iterator")
	}

	return bIt, nil
}