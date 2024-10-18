package catalog

import (
	"encoding/json"
	"helin/btree/btree"
	"helin/common"
	"helin/transaction"
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
	GetStore(txn transaction.Transaction, name string) (*btree.BTree, error)
	ListStores() []string
}
