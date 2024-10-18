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
	locker2 "helin/locker"
	"helin/transaction"
	"strconv"
	"sync"
)

const (
	BtreeDegree = 80
)

var _ Catalog = &PersistentCatalog{}

type PersistentCatalog struct {
	tree strBtree
	pool buffer.Pool
	l    *sync.Mutex
	lm   wal.LogManager
}

func NewPersistentCatalog(tree *btree.BTree, pool buffer.Pool, lm wal.LogManager) *PersistentCatalog {
	return &PersistentCatalog{tree: strBtree{*tree}, pool: pool, l: &sync.Mutex{}, lm: lm}
}

func OpenCatalog(file string, poolSize int, fsync bool) (*PersistentCatalog, buffer.Pool, concurrency.CheckpointManager, concurrency.TxnManager) {
	dbFileName := file + ".helin"
	logDir := file + "_logs"
	segmentSize := uint64(16 * 1024 * 1024)

	dm, created, err := disk.NewDiskManager(dbFileName, fsync)
	common.PanicIfErr(err)

	locker := locker2.NewLockManager()
	if created {
		// TODO: get these as parameters
		lm, err := wal.OpenBWALLogManager(512*1024, segmentSize, logDir, wal.NewDefaultSerDe())
		if err != nil {
			panic(err)
		}

		pool := buffer.NewBufferPoolV2WithDM(true, poolSize, dm, lm, locker)
		tm := concurrency.NewTxnManager(pool, lm, locker, segmentSize, logDir)
		cm := concurrency.NewCheckpointManager(pool, lm, tm)

		// NOTE: maybe use global serializers instead of initializing structs
		pager2 := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
		catalogStore := btree.NewBtreeWithPager(transaction.TxnTODO(), BtreeDegree, pager2)
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

	lm, err := wal.OpenBWALLogManager(512*1024, segmentSize, logDir, wal.NewDefaultSerDe())
	if err != nil {
		panic(err)
	}

	pool := buffer.NewBufferPoolV2WithDM(false, poolSize, dm, lm, locker)
	tm := concurrency.NewTxnManager(pool, lm, locker, segmentSize, logDir)
	cm := concurrency.NewCheckpointManager(pool, lm, tm)

	pager2 := btree.NewPager2(pbtree.NewBufferPoolBPager(pool, lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	catalogStore := btree.ConstructBtreeByMeta(transaction.TxnTODO(), btree.Pointer(dm.GetCatalogPID()), pager2)
	return &PersistentCatalog{
		tree: strBtree{*catalogStore},
		pool: pool,
		l:    &sync.Mutex{},
		lm:   lm,
	}, pool, cm, tm
}

func (p *PersistentCatalog) CreateStore(txn transaction.Transaction, name string) (*StoreInfo, error) {
	if s, err := p.getStoreByName(txn, name); err != nil {
		return nil, err
	} else if s != NullIndexOID {
		return nil, errors.New("already exists")
	}

	oid, err := p.getNextIndexOID(txn)
	if err != nil {
		return nil, err
	}

	if err := p.setStoreByName(txn, name, IndexOID(oid)); err != nil {
		return nil, err
	}

	pager := btree.NewPager2(pbtree.NewBufferPoolBPager(p.pool, p.lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	tree := btree.NewBtreeWithPager(txn, BtreeDegree, pager)

	s := StoreInfo{
		Name:                name,
		KeySerializerType:   0,
		ValueSerializerType: 0,
		Degree:              uint8(BtreeDegree),
		MetaPID:             int64(tree.GetMetaPID()),
	}

	if err := p.setStoreByOID(txn, IndexOID(oid), &s); err != nil {
		return nil, err
	}

	return &s, nil
}

func (p *PersistentCatalog) GetStore(txn transaction.Transaction, name string) (*btree.BTree, error) {
	s, err := p.getStoreByName(txn, name)
	if err != nil {
		return nil, err
	}

	inf, err := p.getStoreByOID(txn, s)
	if err != nil {
		return nil, err
	}
	if inf == nil {
		return nil, nil
	}

	pager := btree.NewPager2(pbtree.NewBufferPoolBPager(p.pool, p.lm), &btree.StringKeySerializer{}, &btree.StringValueSerializer{})
	return btree.ConstructBtreeByMeta(txn, btree.Pointer(inf.MetaPID), pager), nil
}

func (p *PersistentCatalog) ListStores() []string {
	keys := p.tree.ListKeys()
	return keys
}

func (p *PersistentCatalog) getStoreByName(txn transaction.Transaction, name string) (IndexOID, error) {
	key := fmt.Sprintf("index_%v", name)

	s, err := p.tree.Get(txn, key)
	if err != nil {
		return 0, err
	}
	if s == "" {
		return NullIndexOID, nil
	}

	oid, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return NullIndexOID, fmt.Errorf("corrupted catalog: :%w", err)
	}

	return IndexOID(oid), nil
}

func (p *PersistentCatalog) setStoreByName(txn transaction.Transaction, name string, oid IndexOID) error {
	return p.tree.Set(txn, fmt.Sprintf("index_%v", name), fmt.Sprintf("%v", oid))
}

func (p *PersistentCatalog) getStoreByOID(txn transaction.Transaction, id IndexOID) (*StoreInfo, error) {
	if id == NullIndexOID {
		return nil, nil
	}

	key := fmt.Sprintf("index_oid_%v", id)
	d, err := p.tree.Get(txn, key)
	if err != nil {
		return nil, err
	}

	s := StoreInfo{}
	s.Deserialize([]byte(d))
	return &s, nil
}

func (p *PersistentCatalog) setStoreByOID(txn transaction.Transaction, id IndexOID, info *StoreInfo) error {
	key := fmt.Sprintf("index_oid_%v", id)
	return p.tree.Set(txn, key, string(info.Serialize()))
}

func (p *PersistentCatalog) getNextIndexOID(txn transaction.Transaction) (int64, error) {
	// TODO: really implement this
	p.l.Lock()
	lastStr, err := p.tree.Get(txn, "last_id")
	if err != nil {
		return 0, err
	}

	last := 0
	if lastStr != "" {
		var err error
		last, err = strconv.Atoi(lastStr)
		common.PanicIfErr(err)
	}
	last++
	if err := p.tree.Set(txn, "last_id", strconv.Itoa(last)); err != nil {
		return 0, err
	}

	p.l.Unlock()

	return int64(last), nil
}

type strBtree struct {
	btree.BTree
}

func (tree *strBtree) Set(txn transaction.Transaction, key, val string) error {
	_, err := tree.BTree.Set(txn, btree.StringKey(key), val)
	return err
}

func (tree *strBtree) Get(txn transaction.Transaction, key string) (string, error) {
	val, err := tree.BTree.Get(txn, btree.StringKey(key))
	if err != nil {
		return "", err
	}
	if val == nil {
		return "", nil
	}

	return val.(string), nil
}

func (tree *strBtree) ListKeys() []string {
	val := tree.BTree.FindBetween(btree.StringKey("index_oid"), nil, 0)
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
