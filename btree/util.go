package btree

import "helin/common"

type MyInt int

func (key MyInt) Less(than common.Key) bool {
	return key < than.(MyInt)
}

func CheckErr(err error) {
	if err != nil {
		panic(err)
	}
}
