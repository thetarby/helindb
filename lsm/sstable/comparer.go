package sstable

import (
	"bytes"
	"helin/lsm/common"
)

type Comparer interface {
	Compare(a, b []byte) int
	Separator(a, b []byte) []byte
}

var DefaultComparer Comparer = defCmp{}

type defCmp struct{}

func (defCmp) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Separator returns a value v which meets conditions v >= a && v < b, meaning it is a separator between a and b
// Argument a must be smaller than b otherwise returned value is not true. 
// Simplest possible implementation would be func(a, b []byte) {return a} but making result shorter is an optimization 
// when storing tables in disk.
func (defCmp) Separator(a, b []byte) []byte {
	i := common.SharedPrefixLen(a, b)
	separator := make([]byte, len(a))
	copy(separator, a)
	if len(b) > 0 {
		if i == len(a) {
			return separator
		}
		if i == len(b) {
			panic("a < b is a precondition, but b is a prefix of a")
		}
		if a[i] == 0xff || a[i]+1 >= b[i] {
			// TODO: this if is here to avoid making a bigger than b. Otherwise if a is "1345" and b is "2"
			// when we increase a[0] by 1 it is "2345" which is bigger than "2"

			// This isn't optimal, but it matches the C++ Level-DB implementation, and
			// it's good enough. For example, if a is "1357" and b is "2", then the
			// optimal (i.e. shortest) result is appending "14", but we append "1357".
			return separator
		}
	}

	for ; i < len(separator); i++ {
		if separator[i] != 0xff {
			separator[i]++
			return separator[:i+1]
		}
	}
	return separator
}
