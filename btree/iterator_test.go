package btree

import (
	"fmt"
	"helin/buffer"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestTreeIterator_Should_Return_Every_Value_Bigger_Than_Or_Euqal_To_Key_When_Initialized_With_A_Key(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)

	tree := NewBtreeWithPager(10, NewBufferPoolPagerWithValueSize(pool, &StringKeySerializer{Len: 11}, &StringValueSerializer{Len: 11}))
	log.SetOutput(ioutil.Discard)
	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
	}

	it := NewTreeIteratorWithKey(nil, StringKey("selam_099"), tree, tree.pager)
	for i, val := 9900, it.Next(); val != nil; val = it.Next() {
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
		i++
	}

	// all pages should be unpinned after process is finished
	assert.Zero(t, pool.Replacer.NumPinnedPages())
}

func TestTreeIterator_Should_Return_All_Values_When_Initialized_Without_A_Key(t *testing.T) {
	id, _ := uuid.NewUUID()
	dbName := id.String()
	defer os.Remove(dbName)

	pool := buffer.NewBufferPool(dbName, 32)

	tree := NewBtreeWithPager(10, NewBufferPoolPagerWithValueSize(pool, &StringKeySerializer{Len: 11}, &StringValueSerializer{Len: 11}))
	log.SetOutput(ioutil.Discard)
	n := 10000
	for _, i := range rand.Perm(n) {
		tree.Insert(StringKey(fmt.Sprintf("selam_%05d", i)), fmt.Sprintf("value_%05d", i))
	}

	it := NewTreeIterator(nil, tree, tree.pager)
	for i := 0; i < n; i++ {
		val := it.Next()
		assert.Equal(t, fmt.Sprintf("value_%05d", i), val.(string))
	}
	assert.Nil(t, it.Next())
}
