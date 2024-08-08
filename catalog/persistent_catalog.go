package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"helin/btree/btree"
	"helin/btree/pbtree"
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
	degree = 10
)

var _ Catalog = &PersistentCatalog{}

type PersistentCatalog struct {
	tree strBtree
	pool buffer.Pool
	l    *sync.Mutex
	lm   wal.LogManager
}

func OpenCatalog(file string, poolSize int, fsync bool) (*PersistentCatalog, buffer.Pool, concurrency.CheckpointManager, concurrency.TxnManager) {
	dbFileName := file + ".helin"
	logDir := file + "_logs"

	dm, created, err := disk.NewDiskManager(dbFileName, fsync)
	common.PanicIfErr(err)
	if created {
		// TODO: get these as parameters
		lm, err := wal.OpenBWALLogManager(512*1024, 16*1024*1024, logDir, wal.NewJsonLogSerDe())
		if err != nil {
			panic(err)
		}

		pool := buffer.NewBufferPoolV2WithDM(true, poolSize, dm, lm)
		tm := concurrency.NewTxnManager(pool, lm)
		cm := concurrency.NewCheckpointManager(pool, lm, tm)

		// NOTE: maybe use global serializers instead of initializing structs
		pager2 := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		catalogStore := btree.NewBtreeWithPager(transaction.TxnTODO(), degree, pager2)
		dm.SetCatalogPID(uint64(catalogStore.GetMetaPID()))
		if err := pool.FlushAll(); err != nil {
			panic(err)
		}

		return &PersistentCatalog{
			tree: strBtree{*catalogStore},
			pool: pool,
			l:    &sync.Mutex{},
			lm:   lm,
		}, pool, cm, tm
	}

	lm, err := wal.OpenBWALLogManager(512*1024, 16*1024*1024, logDir, wal.NewJsonLogSerDe())
	if err != nil {
		panic(err)
	}

	pool := buffer.NewBufferPoolV2WithDM(false, poolSize, dm, lm)
	tm := concurrency.NewTxnManager(pool, lm)
	cm := concurrency.NewCheckpointManager(pool, lm, tm)

	pager2 := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	catalogStore := btree.ConstructBtreeByMeta(btree.Pointer(dm.GetCatalogPID()), pager2)
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

	pager := btree.NewPager2(pbtree.NewBufferPoolBPager(p.pool, p.lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(txn, degree, pager)

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
	if inf == nil {
		return nil
	}

	pager := btree.NewPager2(pbtree.NewBufferPoolBPager(p.pool, p.lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	return btree.ConstructBtreeByMeta(btree.Pointer(inf.MetaPID), pager)
}

func (p *PersistentCatalog) ListStores() []string {
	keys := p.tree.ListKeys()
	return keys
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
	if id == NullIndexOID {
		return nil
	}

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
	tree.BTree.Set(txn, btree.StringKey(key), val)
}

func (tree *strBtree) Get(key string) string {
	val := tree.BTree.Get(btree.StringKey(key))
	if val == nil {
		return ""
	}

	return tree.BTree.Get(btree.StringKey(key)).(string)
}

func (tree *strBtree) ListKeys() []string {
	val := tree.BTree.FindSince(btree.StringKey("index_oid"))
	if val == nil {
		return nil
	}

	res := make([]string, 0)
	for _, re := range val {
		info := StoreInfo{}
		json.Unmarshal([]byte(re.(string)), &info)
		res = append(res, info.Name)
	}

	return res
}
