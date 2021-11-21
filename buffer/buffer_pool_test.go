package buffer

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"helin/disk"
	"math/rand"
	"os"
	"testing"
)

type teststruct struct {
	Num int
	Val string
}

func TestBuffer_Pool_Should_Write_Pages_To_Disk(t *testing.T) {
	os.Remove("tmp.helin")
	b := NewBufferPool("tmp.helin")
	defer os.Remove("tmp.helin")

	// write 50 pages with 2 sized buffer pool
	for i := 0; i < 50; i++ {
		x := teststruct{Num: i, Val: "selam"}
		json, _ := json.Marshal(x)
		json = append(json, byte('\000'))

		var data [4096]byte
		copy(data[:], json)

		p, err := b.NewPage()
		println(p.GetPageId())
		if err != nil {
			println(err.Error())
		}

		data[4095] = byte('\n')
		p.Data = data[:]

		b.Unpin(p.GetPageId(), true)
	}

	// read each page and validate content
	for i := 0; i < 50; i++ {
		p, err := b.GetPage(i)
		assert.NoError(t, err)

		x := teststruct{}
		byteArr := p.GetData()
		for i := 0; i < len(byteArr); i++ {
			if byteArr[i] == '\000' {
				byteArr = byteArr[:i]
			}
		}
		json.Unmarshal(byteArr, &x)
		assert.Equal(t, i, x.Num)
		assert.Equal(t, "selam", x.Val)
		b.Unpin(p.GetPageId(), false)
	}
}

func TestBuffer_Pool_Should_Not_Corrupt_Pages(t *testing.T) {
	os.Remove("tmp2.helin")
	b := NewBufferPool("tmp2.helin")
	defer os.Remove("tmp2.helin")
	numPagesToTest := 50

	//generate 50 random page sized byte arrays
	randomPages := make([][]byte, 0)
	for i := 0; i < numPagesToTest; i++ {
		randomPage := make([]byte, disk.PageSize)
		rand.Read(randomPage)
		randomPages = append(randomPages, randomPage)
	}

	// write random pages with 10 sized buffer pool
	for i := 0; i < numPagesToTest; i++ {
		p, err := b.NewPage()
		println(p.GetPageId())
		if err != nil {
			println(err.Error())
		}

		p.Data = randomPages[i]

		b.Unpin(p.GetPageId(), true)
	}

	// read each page and validate content
	for i := 0; i < numPagesToTest; i++ {
		p, err := b.GetPage(i)
		assert.NoError(t, err)

		assert.ElementsMatch(t, randomPages[i], p.GetData())
		b.Unpin(p.GetPageId(), false)
	}
}
