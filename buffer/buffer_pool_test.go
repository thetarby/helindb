package buffer

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"helin/common"
	"helin/disk"
	"helin/disk/wal"
	"helin/transaction"
	"io/ioutil"
	"log"
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
	b := NewBufferPool("tmp.helin", 2, wal.NoopLM)
	defer common.Remove("tmp.helin")
	log.SetOutput(ioutil.Discard)

	// write 50 pages with 2 sized buffer pool
	pageIDs := make([]uint64, 0)
	for i := 0; i < 50; i++ {
		x := teststruct{Num: i, Val: "selam"}
		json, _ := json.Marshal(x)
		json = append(json, byte('\000'))

		var data [disk.PageSize]byte
		copy(data[:], json)
		p, err := b.NewPage(transaction.TxnNoop())

		assert.NoError(t, err)
		pageIDs = append(pageIDs, p.GetPageId())

		data[disk.PageSize-1] = byte('\n')
		p.Data = data[:]

		b.Unpin(p.GetPageId(), true)
	}

	// read each page and validate content
	for i, pageID := range pageIDs {
		p, err := b.GetPage(pageID)
		assert.NoError(t, err)

		x := teststruct{}
		byteArr := p.GetWholeData()
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
	b := NewBufferPool("tmp2.helin", 2, wal.NoopLM)
	defer common.Remove("tmp2.helin")
	log.SetOutput(ioutil.Discard)

	numPagesToTest := 50

	//generate 50 random page sized byte arrays
	randomPages := make([][]byte, 0)
	for i := 0; i < numPagesToTest; i++ {
		randomPage := make([]byte, disk.PageSize)
		rand.Read(randomPage)
		randomPages = append(randomPages, randomPage)
	}

	// write random pages with 10 sized buffer pool
	pageIDs := make([]uint64, 0)
	for i := 0; i < numPagesToTest; i++ {
		p, err := b.NewPage(transaction.TxnNoop())
		pageIDs = append(pageIDs, p.GetPageId())
		require.NoError(t, err)

		n := copy(p.GetWholeData(), randomPages[i])
		require.Equal(t, n, len(randomPages[i]))

		b.Unpin(p.GetPageId(), true)
	}

	// read each page and validate content
	for i := 0; i < numPagesToTest; i++ {
		p, err := b.GetPage(pageIDs[i])
		assert.NoError(t, err)

		assert.ElementsMatch(t, randomPages[i], p.GetWholeData())
		b.Unpin(p.GetPageId(), false)
	}
}
