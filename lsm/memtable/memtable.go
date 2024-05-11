package memtable

type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Close() error
}

type MemTable struct{

}

func (m *MemTable) Get(key []byte) ([]byte, error){
	panic("implement me")
}

func (m *MemTable) Set(key, val []byte) error{
	panic("implement me")
}

func (m *MemTable) Delete(key []byte) error{
	panic("implement me")
}

func (m *MemTable) Find(key []byte) Iterator{
	panic("implement me")
}

func (m *MemTable) Close() error{
	panic("implement me")
}