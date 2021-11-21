package btree

type MyInt int

func (key MyInt) Less(than Key) bool {
	return key < than.(MyInt)
}

func (n *InternalNode) truncate(index int) {
	n.Keys = n.Keys[:index]
	n.Pointers = n.Pointers[:index+1]
}

func CheckErr(err error){
	if err != nil{
		panic(err.Error())
	}
}