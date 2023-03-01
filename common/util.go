package common

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
)

func PanicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

// Contains tells whether arr contains x.
func Contains(arr []int, x int) bool {
	for _, n := range arr {
		if x == n {
			return true
		}
	}
	return false
}

func IndexOfInt(element int, data []int) int {
	for k, v := range data {
		if element == v {
			return k
		}
	}
	return -1 //not found.
}

func ChunksInt(arr []int, chunkSize int) [][]int {
	var res [][]int
	for i := 0; i < len(arr); i += chunkSize {
		end := i + chunkSize
		if end > len(arr) {
			end = len(arr)
		}

		res = append(res, arr[i:end])
	}

	return res
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStr(min, max uint) string {
	n := rand.Intn(int(max-min)) + int(min)
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func Clone[T any](v []T) []T {
	var clone = make([]T, len(v))
	copy(clone, v)
	return clone
}

func Ternary[T any](exp bool, a, b T) T {
	if exp {
		return a
	}
	return b
}

func Reverse[T any](a []T) []T {
	l := len(a)
	r := make([]T, 0)
	for i := 0; i < l; i++ {
		r = append(r, a[l-i-1])
	}

	return r
}

func OneOf[T comparable](a T, l ...T) bool {
	for i := 0; i < len(l); i++ {
		if a == l[i] {
			return true
		}
	}

	return false
}

func Remove(dbName string) {
	PanicIfErr(os.Remove(dbName))
	PanicIfErr(os.Remove(dbName + ".log"))
}

var _ io.Reader = &StatReader{}

type StatReader struct {
	r         io.Reader
	TotalRead int
}

func (s *StatReader) Read(p []byte) (n int, err error) {
	n, err = s.r.Read(p)
	s.TotalRead += n
	return
}

func NewStatReader(r io.Reader) *StatReader {
	return &StatReader{r: r}
}

func Uint64AsBytes(x uint64) []byte {
	// OPTIMIZATION NOTE: heap alloc
	res := make([]byte, 8)
	binary.BigEndian.PutUint64(res, x)
	return res
}

func Assert(condition bool, msg string, v ...any) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
