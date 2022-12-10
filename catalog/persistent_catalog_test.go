package catalog

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"helin/btree"
	"testing"
)

func TestName(t *testing.T) {
	ctl, pool := OpenCatalog("test.helin", 50)
	_, err := ctl.CreateStore(nil, "store1")
	require.NoError(t, err)
	tree := ctl.GetStore(nil, "store1")
	tree.Insert(btree.StringKey("key1"), "val1")
	err = pool.FlushAll()
	require.NoError(t, err)
}

func TestName2(t *testing.T) {
	ctl, pool := OpenCatalog("test.helin", 50)
	tree := ctl.GetStore(nil, "store1")
	tree.Insert(btree.StringKey("key2"), "val2")
	require.NoError(t, pool.FlushAll())
}

func TestName3(t *testing.T) {
	ctl, pool := OpenCatalog("test.helin", 50)
	tree := ctl.GetStore(nil, "store1")
	for i := 0; i < 10000; i++ {
		tree.InsertOrReplace(btree.StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("val_%v", i))
	}
	require.NoError(t, pool.FlushAll())
}

func TestName4(t *testing.T) {
	ctl, _ := OpenCatalog("test.helin", 50)
	tree := ctl.GetStore(nil, "store1")
	v := tree.Find(btree.StringKey("key2"))
	println(v.(string))
}

func TestName5(t *testing.T) {
	ctl, _ := OpenCatalog("test.helin", 50)
	tree := ctl.GetStore(nil, "store1")
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%v", i)
		v := tree.Find(btree.StringKey(key))
		t.Logf("key: %v, val: %v", key, v.(string))
	}
}

func TestAnotherStore(t *testing.T) {
	ctl, pool := OpenCatalog("test.helin", 50)
	_, err := ctl.CreateStore(nil, "store2")
	require.NoError(t, err)
	require.NoError(t, pool.FlushAll())
}

func TestAnotherStore2(t *testing.T) {
	ctl, pool := OpenCatalog("test.helin", 50)
	tree := ctl.GetStore(nil, "store2")
	for i := 0; i < 10000; i++ {
		tree.InsertOrReplace(btree.StringKey(fmt.Sprintf("key_%v", i)), fmt.Sprintf("valv2_%v", i))
	}
	require.NoError(t, pool.FlushAll())
}

func TestAnotherStore3(t *testing.T) {
	ctl, _ := OpenCatalog("test.helin", 50)
	tree := ctl.GetStore(nil, "store2")
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%v", i)
		v := tree.Find(btree.StringKey(key))
		t.Logf("key: %v, val: %v", key, v.(string))
	}
}
