package common

import (
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

func Reverse[T any](a []T) []T {
	l := len(a)
	r := make([]T, 0)
	for i := 0; i < l; i++ {
		r = append(r, a[l-i-1])
	}

	return r
}

func Remove(dbName string) {
	os.Remove(dbName)
	os.Remove(dbName + ".log")
}
