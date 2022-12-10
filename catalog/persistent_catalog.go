package catalog

import (
	"errors"
	"fmt"
	"helin/btree"
	"helin/buffer"
	"helin/common"
	"helin/concurrency"
	"helin/disk"
	"strconv"
	"sync"
)

const (
	degree = 50
)

var _ Catalog = &PersistentCatalog{}

type PersistentCatalog struct {
	tree strBtree
	pool *buffer.BufferPool
	l    *sync.Mutex
}

func OpenCatalog(file string, poolSize int) (*PersistentCatalog, buffer.IBufferPool) {
	dm, created, err := disk.NewDiskManager(file)
	common.PanicIfErr(err)
	if created {
		pool := buffer.NewBufferPoolWithDM(poolSize, dm)
		// NOTE: maybe use global serializers instead of initializing structs
		bpp := btree.NewBPP(pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		catalogStore := btree.NewBtreeWithPager(degree, bpp)
		dm.SetCatalogPID(uint64(catalogStore.GetMetaPID()))
		return &PersistentCatalog{
			tree: strBtree{*catalogStore},
			pool: pool,
			l:    &sync.Mutex{},
		}, pool
	}

	pool := buffer.NewBufferPoolWithDM(poolSize, dm)
	bpp := btree.NewBPP(pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	catalogStore := btree.ConstructBtreeByMeta(btree.Pointer(dm.GetCatalogPID()), bpp)
	return &PersistentCatalog{
		tree: strBtree{*catalogStore},
		pool: pool,
		l:    &sync.Mutex{},
	}, pool
}

func (p *PersistentCatalog) CreateStore(txn concurrency.Transaction, name string) (*StoreInfo, error) {
	if s := p.getStoreByName(name); s != NullIndexOID {
		return nil, errors.New("already exists")
	}

	oid := p.getNextIndexOID()
	p.setStoreByName(name, IndexOID(oid))

	pager := btree.NewBPP(p.pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(degree, pager)

	s := StoreInfo{
		Name:                name,
		KeySerializerType:   0,
		ValueSerializerType: 0,
		Degree:              uint8(degree),
		MetaPID:             int64(tree.GetMetaPID()),
	}
	p.setStoreByOID(IndexOID(oid), &s)

	return &s, nil
}

func (p *PersistentCatalog) GetStore(txn concurrency.Transaction, name string) *btree.BTree {
	inf := p.getStoreByOID(p.getStoreByName(name))

	pager := btree.NewBPP(p.pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	return btree.ConstructBtreeByMeta(btree.Pointer(inf.MetaPID), pager)
}

func (p *PersistentCatalog) CreateTable(txn concurrency.Transaction, tableName string, schema Schema) *TableInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentCatalog) GetTable(name string) *TableInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentCatalog) GetTableByOID(oid TableOID) *TableInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentCatalog) CreateBtreeIndex(txn concurrency.Transaction, indexName string, tableName string, columnIndexes []int, isUnique bool) (*IndexInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentCatalog) GetIndexByOID(indexOID IndexOID) *IndexInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentCatalog) GetTableIndexes(tableName string) []IndexInfo {
	//TODO implement me
	panic("implement me")
}

func (p *PersistentCatalog) getStoreByName(name string) IndexOID {
	key := fmt.Sprintf("index_%v", name)
	s := p.tree.Get(key)
	if s == "" {
		return NullIndexOID
	}

	oid, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		panic("corrupted catalog: " + err.Error())
	}

	return IndexOID(oid)
}

func (p *PersistentCatalog) setStoreByName(name string, oid IndexOID) {
	p.tree.Set(fmt.Sprintf("index_%v", name), fmt.Sprintf("%v", oid))
}

func (p *PersistentCatalog) getStoreByOID(id IndexOID) *StoreInfo {
	key := fmt.Sprintf("index_oid_%v", id)
	s := StoreInfo{}
	s.Deserialize([]byte(p.tree.Get(key)))
	return &s
}

func (p *PersistentCatalog) setStoreByOID(id IndexOID, info *StoreInfo) {
	key := fmt.Sprintf("index_oid_%v", id)
	p.tree.Set(key, string(info.Serialize()))
}

func (p *PersistentCatalog) getNextIndexOID() int64 {
	// TODO: really implement this
	p.l.Lock()
	lastStr := p.tree.Get("last_id")

	last := 0
	if lastStr != "" {
		var err error
		last, err = strconv.Atoi(lastStr)
		common.PanicIfErr(err)
	}
	last++
	p.tree.Set("last_id", strconv.Itoa(last))
	p.l.Unlock()

	return int64(last)
}

type strBtree struct {
	btree.BTree
}

func (tree *strBtree) Set(key, val string) {
	tree.InsertOrReplace(btree.StringKey(key), val)
}

func (tree *strBtree) Get(key string) string {
	val := tree.Find(btree.StringKey(key))
	if val == nil {
		return ""
	}

	return tree.Find(btree.StringKey(key)).(string)
}
