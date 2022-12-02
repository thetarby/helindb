package catalog

import (
	"fmt"
	"helin/btree"
	"helin/buffer"
	"helin/catalog/db_types"
	"helin/common"
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
	catalog *InMemCatalog
}

//tree := NewBtreeWithPager(50, NewBufferPoolPagerWithValueSerializer(pool, &StringKeySerializer{}, &StringValueSerializer{}))

type IndexInfo struct {
	Index   *btree.BTree
	catalog *InMemCatalog

	IndexName string
	OID       IndexOID
	IsUnique  bool

	// below fields are necessary if this is a managed index
	Schema        Schema
	BareSchema    Schema
	TableName     string
	ColumnIndexes []int
}

type TableOID uint32
type IndexOID uint32

const NullTableOID TableOID = 0
const NullIndexOID IndexOID = 0

type Catalog interface {
	CreateTable(txn concurrency.Transaction, tableName string, schema Schema) *TableInfo
	GetTable(name string) *TableInfo
	GetTableByOID(oid TableOID) *TableInfo

	CreateBtreeIndex(txn concurrency.Transaction, indexName string, tableName string, columnIndexes []int, isUnique bool) (*IndexInfo, error)
	GetIndex(indexName, tableName string) *IndexInfo
	GetIndexByOID(indexOID IndexOID) *IndexInfo
	GetIndexByTableOID(indexName, oid TableOID) *IndexInfo
	GetTableIndexes(tableName string) []IndexInfo
}

type InMemCatalog struct {
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

func (c *InMemCatalog) CreateTable(txn concurrency.Transaction, tableName string, schema Schema) *TableInfo {
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

func (c *InMemCatalog) GetTable(name string) *TableInfo {
	oid, ok := c.tableNames[name]
	if !ok {
		return nil
	}

	return c.tables[oid]
}

func (c *InMemCatalog) GetTableByOID(oid TableOID) *TableInfo {
	return c.tables[oid]
}

func (c *InMemCatalog) CreateBtreeIndex(txn concurrency.Transaction, indexName string, tableName string, columnIndexes []int, isUnique bool) (*IndexInfo, error) {
	// keySchema can be generated from columnIndexes
	if c.tableNames[tableName] == 0 {
		return nil, fmt.Errorf("tried to create an index on a nonexistent table: %v", tableName)
	}

	indexesOnTable := c.indexNames[tableName]
	if indexesOnTable[indexName] != 0 {
		return nil, fmt.Errorf("an index with the same name is already defined on the table. table: %v, index: %v", tableName, indexName)
	}

	// generate key schema
	table := c.GetTable(tableName)
	tableCols := table.Schema.GetColumns()
	indexCols := make([]Column, 0)
	for _, index := range columnIndexes {
		indexCols = append(indexCols, tableCols[index])
	}

	bareSchema := NewSchema(indexCols)
	if !isUnique {
		indexCols = append(indexCols, NewColumn("page_id", db_types.IntegerTypeID),
			NewColumn("slot_idx", db_types.IntegerTypeID))
	}

	keySchema := NewSchema(indexCols)
	serializer := TupleKeySerializer{schema: keySchema}

	// TODO: what should be degree?
	index := btree.NewBtreeWithPager(50, btree.NewBufferPoolPager(c.pool, &serializer))
	it := structures.NewTableIterator(txn, table.Heap)
	for {
		n := CastRowAsTuple(it.Next())
		if n == nil {
			break
		}

		vals := make([]*db_types.Value, 0)
		for _, idx := range columnIndexes {
			val := n.GetValue(table.Schema, idx)
			vals = append(vals, val)
		}

		if !isUnique { // if it is not a unique index append rid to make keys unique
			vals = append(vals, db_types.NewValue(int32(n.Rid.PageId)), db_types.NewValue(int32(n.Rid.SlotIdx))) // TODO: pageID is uint64 there might be overflow
		}

		t, err := NewTupleWithSchema(vals, keySchema)
		common.PanicIfErr(err)

		tupleKey := TupleKey{
			Schema: keySchema,
			Tuple:  *t,
		}
		index.Insert(&tupleKey, n.Rid)
	}

	oid := c.getNextIndexOID()
	info := IndexInfo{
		Schema:        keySchema,
		BareSchema:    bareSchema,
		IndexName:     indexName,
		TableName:     tableName,
		OID:           oid,
		Index:         index,
		catalog:       c,
		ColumnIndexes: columnIndexes,
		IsUnique:      isUnique,
	}
	c.indexes[oid] = &info
	indexesOnTable[indexName] = oid
	return &info, nil
}

func (c *InMemCatalog) GetIndex(indexName, tableName string) *IndexInfo {
	panic("implement me")
}

func (c *InMemCatalog) GetIndexByOID(indexOID IndexOID) *IndexInfo {
	return c.indexes[indexOID]
}

func (c *InMemCatalog) GetIndexByTableOID(indexName, oid TableOID) *IndexInfo {
	panic("implement me")
}

func (c *InMemCatalog) GetTableIndexes(tableName string) []IndexInfo {
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

func NewCatalog(pool *buffer.BufferPool) Catalog {
	return &InMemCatalog{
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
