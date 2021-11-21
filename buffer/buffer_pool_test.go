package buffer

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"helin/disk/pages"
	"os"
	"testing"
)

type teststruct struct {
	Num int
	Val string
}

func TestBufferPoolShouldWritePagesToDisk(t *testing.T) {
	os.Remove("tmp.helin")
	b := NewBufferPool("tmp.helin", 2)
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
		p.(*pages.RawPage).Data = data[:]

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
