package sstable

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriter_Should_Throw_Error_When_Called_With_Non_Increasing_Keys(t *testing.T){
	buf := bytes.Buffer{}
	w := NewWriter(&buf, DefaultComparer)
	assert.NoError(t, w.Set(b("a"), b("a" )))
	assert.Error(t, w.Set(b("a"), b("a" )))

	buf2 := bytes.Buffer{}
	w2 := NewWriter(&buf2, DefaultComparer)
	assert.NoError(t, w2.Set(b("b"), b("b" )))
	assert.Error(t, w2.Set(b("a"), b("a" )))
}

func TestWriter(t *testing.T){
	file, err := ioutil.TempFile("", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	
	w := NewWriter(file, DefaultComparer)
	n := 10000
	for i := 0; i < n; i++ {
		str := fmt.Sprintf("%05d", i)
		if err := w.Set(b(str), b(fmt.Sprintf("val_%05d", i))); err != nil {
			t.Error(err)
		}		
	}

	assert.NoError(t, w.Close())

	reader, err := NewReader(file, DefaultComparer)
	assert.NoError(t, err)
	for i := 0; i < n; i++ {
		str := fmt.Sprintf("%05d", i)
		val, err := reader.Get(b(str))
		if err != nil{
			t.Error(err)
		}

		assert.Equal(t, b(fmt.Sprintf("val_%05d", i)), val)
	}

	_, err = reader.Get(b("selam"))
	assert.ErrorIs(t, err, NotFound)
}

func TestWriter_Table_Iter(t *testing.T){
	file, err := ioutil.TempFile("", "prefix")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(file.Name())
	
	w := NewWriter(file, DefaultComparer)
	
	// generate random key value pairs
	n := 10000
	hash := make(map[string][]byte)
	for i := 0; i < n; i++ {
		key := RandStringBytes(5, 50)
		val := RandStringBytes(5, 50)
		hash[string(key)]=val
	}

	// sort keys
	keys := make([]string, 0, len(hash))
	for k := range hash {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// write keys sorted
	for _, k := range keys {
		if err := w.Set(b(k), hash[k]); err != nil {
			t.Error(err)
		}
	}

	assert.NoError(t, w.Close())

	// assert all keys exists
	reader, err := NewReader(file, DefaultComparer)
	assert.NoError(t, err)
	for k, v := range hash{
		val, err := reader.Get(b(k))
		if err != nil{
			t.Error(err)
		}

		assert.Equal(t, v, val)
	}

	_, err = reader.Get(b("selam"))
	assert.ErrorIs(t, err, NotFound)
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func RandStringBytes(min, max int) []byte {
	l := rand.Intn(max - min)
	l += min
    b := make([]byte, l)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return b
}

func TestWriter_Table_Iter2(t *testing.T){
	n := 10000
	for i := 0; i < n; i++ {
		println(string(RandStringBytes(2, 10)))	
	}
}