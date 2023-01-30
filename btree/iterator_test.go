package btree

import (
	"fmt"
	"helin/buffer"
	"helin/common"
	"helin/disk/wal"
	"helin/transaction"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTreeIterator_Should_Return_Every_Value_Bigger_Than_Or_Euqal_To_Key_When_Initialized_With_A_Key(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)

	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewBPP(pool, &StringKeySerializer{}, &StringValueSerializer{}, wal.NewLogManager(io.Discard)))
	log.SetOutput(ioutil.Discard)
	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
		assert.Zero(t, pool.Replacer.NumPinnedPages())
	}

	it := NewTreeIteratorWithKey(transaction.TxnNoop(), StringKey("selam_099"), tree, tree.pager)
	i := 9900
	for _, val := it.Next(); val != nil; _, val = it.Next() {
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
		i++
	}

	// all pages should be unpinned after process is finished
	assert.Zero(t, pool.Replacer.NumPinnedPages())
}

func TestTreeIterator_Should_Return_All_Values_When_Initialized_Without_A_Key(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer common.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)

	tree := NewBtreeWithPager(transaction.TxnNoop(), 10, NewBPP(pool, &StringKeySerializer{}, &StringValueSerializer{}, wal.NewLogManager(io.Discard)))
	log.SetOutput(ioutil.Discard)
	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(transaction.TxnNoop(), StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
	}

	it := NewTreeIterator(transaction.TxnNoop(), tree, tree.pager)
	for i := 0; i < n; i++ {
		_, val := it.Next()
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
	}
	_, val := it.Next()
	assert.Nil(t, val)
}
