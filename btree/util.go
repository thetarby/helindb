package btree

type MyInt int

func (key MyInt) Less(than Key) bool {
	return key < than.(MyInt)
}

func CheckErr(err error){
	if err != nil{
		panic(err.Error())
	}
}