package catalog

import (
	"encoding/json"
	"helin/btree/btree"
	"helin/buffer"
	"helin/common"
	"helin/transaction"
	"sync"
)

type StoreInfo struct {
	Name                string
	KeySerializerType   uint8
	ValueSerializerType uint8
	Degree              uint8
	MetaPID             int64
}

func (s *StoreInfo) Serialize() []byte {
	b, err := json.Marshal(s)
	common.PanicIfErr(err)
	return b
}

func (s *StoreInfo) Deserialize(b []byte) {
	err := json.Unmarshal(b, s)
	common.PanicIfErr(err)
}

type TableOID uint32
type IndexOID uint32

const NullIndexOID IndexOID = 0

type Catalog interface {
	CreateStore(txn transaction.Transaction, name string) (*StoreInfo, error)
	GetStore(txn transaction.Transaction, name string) *btree.BTree
	ListStores() []string
}

var _ Catalog = &InMemCatalog{}

type InMemCatalog struct {
	// indexNames tableName => indexName => indexOID
	indexNames map[string]map[string]IndexOID

	nextTableOID TableOID
	tableOIDLock *sync.Mutex

	nextIndexOID IndexOID
	indexOIDLock *sync.Mutex

	pool buffer.Pool
}

func (c *InMemCatalog) CreateStore(txn transaction.Transaction, name string) (*StoreInfo, error) {
	//TODO implement me
	panic("implement me")
}

func (c *InMemCatalog) GetStore(txn transaction.Transaction, name string) *btree.BTree {
	//TODO implement me
	panic("implement me")
}

func (c *InMemCatalog) ListStores() []string {
	//TODO implement me
	panic("implement me")
}

func (c *InMemCatalog) getNextTableOID() TableOID {
	c.tableOIDLock.Lock()
	defer c.tableOIDLock.Unlock()
	c.nextTableOID++
	return c.nextTableOID
}

func (c *InMemCatalog) getNextIndexOID() IndexOID {
	c.indexOIDLock.Lock()
	defer c.indexOIDLock.Unlock()
	c.nextIndexOID++
	return c.nextIndexOID
}

func NewCatalog(pool buffer.Pool) Catalog {
	return &InMemCatalog{
		indexNames:   make(map[string]map[string]IndexOID),
		nextTableOID: 0,
		tableOIDLock: &sync.Mutex{},
		nextIndexOID: 0,
		indexOIDLock: &sync.Mutex{},
		pool:         pool,
	}
}
