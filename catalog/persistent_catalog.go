package catalog

import (
	"errors"
	"fmt"
	"helin/btree"
	"helin/buffer"
	"helin/common"
	"helin/concurrency"
	"helin/disk"
	"helin/disk/wal"
	"helin/transaction"
	"strconv"
	"sync"
)

const (
	degree = 70
)

var _ Catalog = &PersistentCatalog{}

type PersistentCatalog struct {
	tree strBtree
	pool *buffer.BufferPool
	l    *sync.Mutex
	lm   wal.LogManager
}

func OpenCatalog(file string, poolSize int) (*PersistentCatalog, buffer.IBufferPool, concurrency.CheckpointManager, concurrency.TxnManager) {
	dm, created, err := disk.NewDiskManager(file)
	common.PanicIfErr(err)
	if created {
		lm := wal.NewLogManager(dm.GetLogWriter())
		lm.RunFlusher()
		pool := buffer.NewBufferPoolWithDM(true, poolSize, dm, lm)
		tm := concurrency.NewTxnManager(pool, lm)
		cm := concurrency.NewCheckpointManager(pool, lm, tm)

		// NOTE: maybe use global serializers instead of initializing structs
		bpp := btree.NewBPP(pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{}, lm)
		catalogStore := btree.NewBtreeWithPager(transaction.TxnNoop(), degree, bpp)
		dm.SetCatalogPID(uint64(catalogStore.GetMetaPID()))
		return &PersistentCatalog{
			tree: strBtree{*catalogStore},
			pool: pool,
			l:    &sync.Mutex{},
			lm:   lm,
		}, pool, cm, tm
	}

	lm := wal.NewLogManager(dm.GetLogWriter())
	lm.RunFlusher()
	pool := buffer.NewBufferPoolWithDM(true, poolSize, dm, lm)
	tm := concurrency.NewTxnManager(pool, lm)
	cm := concurrency.NewCheckpointManager(pool, lm, tm)

	bpp := btree.NewBPP(pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{}, lm)
	catalogStore := btree.ConstructBtreeByMeta(btree.Pointer(dm.GetCatalogPID()), bpp)
	return &PersistentCatalog{
		tree: strBtree{*catalogStore},
		pool: pool,
		l:    &sync.Mutex{},
		lm:   lm,
	}, pool, cm, tm
}

func (p *PersistentCatalog) CreateStore(txn transaction.Transaction, name string) (*StoreInfo, error) {
	if s := p.getStoreByName(name); s != NullIndexOID {
		return nil, errors.New("already exists")
	}

	oid := p.getNextIndexOID(txn)
	p.setStoreByName(txn, name, IndexOID(oid))

	pager := btree.NewBPP(p.pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{}, p.lm)
	tree := btree.NewBtreeWithPager(transaction.TxnNoop(), degree, pager)

	s := StoreInfo{
		Name:                name,
		KeySerializerType:   0,
		ValueSerializerType: 0,
		Degree:              uint8(degree),
		MetaPID:             int64(tree.GetMetaPID()),
	}
	p.setStoreByOID(txn, IndexOID(oid), &s)

	return &s, nil
}

func (p *PersistentCatalog) GetStore(txn transaction.Transaction, name string) *btree.BTree {
	inf := p.getStoreByOID(p.getStoreByName(name))

	pager := btree.NewBPP(p.pool, &btree.StringKeySerializer{}, &btree.StringValueSerializer{}, p.lm)
	return btree.ConstructBtreeByMeta(btree.Pointer(inf.MetaPID), pager)
}

func (p *PersistentCatalog) CreateTable(txn transaction.Transaction, tableName string, schema Schema) *TableInfo {
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

func (p *PersistentCatalog) CreateBtreeIndex(txn transaction.Transaction, indexName string, tableName string, columnIndexes []int, isUnique bool) (*IndexInfo, error) {
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

func (p *PersistentCatalog) setStoreByName(txn transaction.Transaction, name string, oid IndexOID) {
	p.tree.Set(txn, fmt.Sprintf("index_%v", name), fmt.Sprintf("%v", oid))
}

func (p *PersistentCatalog) getStoreByOID(id IndexOID) *StoreInfo {
	key := fmt.Sprintf("index_oid_%v", id)
	s := StoreInfo{}
	s.Deserialize([]byte(p.tree.Get(key)))
	return &s
}

func (p *PersistentCatalog) setStoreByOID(txn transaction.Transaction, id IndexOID, info *StoreInfo) {
	key := fmt.Sprintf("index_oid_%v", id)
	p.tree.Set(txn, key, string(info.Serialize()))
}

func (p *PersistentCatalog) getNextIndexOID(txn transaction.Transaction) int64 {
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
	p.tree.Set(txn, "last_id", strconv.Itoa(last))
	p.l.Unlock()

	return int64(last)
}

type strBtree struct {
	btree.BTree
}

func (tree *strBtree) Set(txn transaction.Transaction, key, val string) {
	tree.InsertOrReplace(txn, btree.StringKey(key), val)
}

func (tree *strBtree) Get(key string) string {
	val := tree.Find(btree.StringKey(key))
	if val == nil {
		return ""
	}

	return tree.Find(btree.StringKey(key)).(string)
}
