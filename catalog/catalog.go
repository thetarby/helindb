package catalog

import (
	"helin/btree"
	"helin/buffer"
	"helin/concurrency"
	"helin/disk/structures"
	"log"
	"sync"
)

type TableInfo struct {
	Schema  Schema
	Name    string
	Heap    *structures.TableHeap
	OID     TableOID
	catalog *Catalog
}

type IndexInfo struct {
	Schema        Schema
	IndexedColIdx int // for now an index can be created only on one field. this is that field. When schema IndexInfo.Schema is used this will be useless
	IndexName     string
	TableName     string
	OID           IndexOID
	KeySize       int
	Index         *btree.BTree
	catalog       *Catalog
}

type TableOID uint32
type IndexOID uint32

const NullTableOID TableOID = 0
const NullIndexOID IndexOID = 0

type ICatalog interface {
	CreateTable(txn concurrency.Transaction, tableName string, schema Schema) *TableInfo
	GetTable(name string) *TableInfo
	GetTableByOID(oid TableOID) *TableInfo

	CreateBtreeIndex(txn concurrency.Transaction, indexName string, tableName string, schema Schema, keySize int, serializer btree.KeySerializer, columnIdx int) *IndexInfo
	GetIndex(indexName, tableName string) *IndexInfo
	GetIndexByOID(indexOID IndexOID) *IndexInfo
	GetIndexByTableOID(indexName, oid TableOID) *IndexInfo
	GetTableIndexes(tableName string) []IndexInfo
}

type Catalog struct {
	tables     map[TableOID]*TableInfo
	tableNames map[string]TableOID

	indexes map[IndexOID]*IndexInfo

	// indexNames tableName => indexName => indexOID
	indexNames map[string]map[string]IndexOID

	nextTableOID TableOID
	tableOIDLock *sync.Mutex

	nextIndexOID IndexOID
	indexOIDLock *sync.Mutex

	pool *buffer.BufferPool
}

func (c *Catalog) CreateTable(txn concurrency.Transaction, tableName string, schema Schema) *TableInfo {
	if c.tableNames[tableName] != NullTableOID {
		return nil
	}

	heap, err := structures.NewTableHeapWithTxn(c.pool, txn)
	if err != nil {
		log.Print(err)
		return nil
	}

	tableOID := c.getNextTableOID()
	info := TableInfo{
		Schema:  schema,
		Name:    tableName,
		Heap:    heap,
		OID:     tableOID,
		catalog: c,
	}

	c.tables[tableOID] = &info
	c.tableNames[tableName] = tableOID
	c.indexNames[tableName] = map[string]IndexOID{}

	return &info
}

func (c *Catalog) GetTable(name string) *TableInfo {
	oid, ok := c.tableNames[name]
	if !ok {
		return nil
	}

	return c.tables[oid]
}

func (c *Catalog) GetTableByOID(oid TableOID) *TableInfo {
	panic("implement me")
}

func (c *Catalog) CreateBtreeIndex(txn concurrency.Transaction, indexName string, tableName string, schema Schema, keySize int, serializer btree.KeySerializer, columnIdx int) *IndexInfo {
	if c.tableNames[tableName] == 0 {
		log.Printf("tried to create an index on a nonexistent table: %v", tableName)
		return nil
	}

	indexesOnTable := c.indexNames[tableName]
	if indexesOnTable[indexName] != 0 {
		log.Printf("an index with the same name is already defined on the table. table: %v, index: %v", tableName, indexName)
		return nil
	}

	index := btree.NewBtreeWithPager(10, btree.NewBufferPoolPager(c.pool, serializer))
	table := c.GetTable(tableName)
	it := structures.NewTableIterator(txn, table.Heap)
	for {
		n := CastRowAsTuple(it.Next())
		if n == nil {
			break
		}

		key := n.GetValue(schema, columnIdx)
		index.Insert(key, n.Rid)
	}

	oid := c.getNextIndexOID()
	info := IndexInfo{
		Schema:        schema,
		IndexedColIdx: columnIdx,
		IndexName:     indexName,
		TableName:     tableName,
		OID:           oid,
		KeySize:       keySize,
		Index:         index,
		catalog:       c,
	}
	c.indexes[oid] = &info
	indexesOnTable[indexName] = oid
	return &info
}

func (c *Catalog) GetIndex(indexName, tableName string) *IndexInfo {
	panic("implement me")
}

func (c *Catalog) GetIndexByOID(indexOID IndexOID) *IndexInfo {
	panic("implement me")
}

func (c *Catalog) GetIndexByTableOID(indexName, oid TableOID) *IndexInfo {
	panic("implement me")
}

func (c *Catalog) GetTableIndexes(tableName string) []IndexInfo {
	panic("implement me")
}

func (c *Catalog) getNextTableOID() TableOID {
	c.tableOIDLock.Lock()
	defer c.tableOIDLock.Unlock()
	c.nextTableOID++
	return c.nextTableOID
}

func (c *Catalog) getNextIndexOID() IndexOID {
	c.indexOIDLock.Lock()
	defer c.indexOIDLock.Unlock()
	c.nextIndexOID++
	return c.nextIndexOID
}

func NewCatalog(pool *buffer.BufferPool) ICatalog {
	return &Catalog{
		tables:       make(map[TableOID]*TableInfo),
		tableNames:   make(map[string]TableOID),
		indexes:      make(map[IndexOID]*IndexInfo),
		indexNames:   make(map[string]map[string]IndexOID),
		nextTableOID: 0,
		tableOIDLock: &sync.Mutex{},
		nextIndexOID: 0,
		indexOIDLock: &sync.Mutex{},
		pool:         pool,
	}
}
