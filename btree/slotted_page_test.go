package btree

import (
	"fmt"
	"helin/disk/pages"
	"testing"

	"github.com/stretchr/testify/assert"
)

func newSp() *SlottedPage{
	sp :=  &SlottedPage{
		PersistentPage: PersistentPage{
			RawPage: pages.RawPage{
				PinCount: 0,
				Data:     make([]byte, 4096),
			},
		},
	}

	sp.SetHeader(SlottedPageHeader{
		FreeSpacePointer: uint16(len(sp.Data)),
		SlotArrSize:      0,
	})
	
	return sp
}

func b(s string) []byte{
	return []byte(s)
}

func TestSlottedPage(t *testing.T) {
	sp := newSp()
	for i := 0; i < 10; i++ {
		sp.InsertAt(i, b(fmt.Sprintf("selam_%v", i)))
	}

	for i := 0; i < 10; i++ {
		res := sp.GetAt(i)
		assert.Equal(t, b(fmt.Sprintf("selam_%v", i)), res)
	}

	t.Logf("value at fifth index: %v", string(sp.GetAt(5)))
	assert.Equal(t, b("selam_5"), sp.GetAt(5))
	sp.DeleteAt(5)
	sp.DeleteAt(5)
	t.Logf("value at fifth index after deletes: %v", string(sp.GetAt(5)))

	t.Log(string(sp.values()))
	assert.Equal(t, b("\x07selam_9\x07selam_8\x07selam_7\x07selam_6\x07selam_5\x07selam_4\x07selam_3\x07selam_2\x07selam_1\x07selam_0"), sp.values())
	
	sp.Vacuum()

	t.Log(string(sp.values()))
	assert.Equal(t, b("\x07selam_9\x07selam_8\x07selam_7\x07selam_4\x07selam_3\x07selam_2\x07selam_1\x07selam_0"), sp.values())
}

func TestSlottedPage_Vacuum(t *testing.T) {
	sp := newSp()
	for i := 0; i < 5; i++ {
		sp.InsertAt(i, b(fmt.Sprintf("selam_%v", i)))
	}

	assert.Equal(t, float32(1), sp.FillFactor())

	for i := 0; i < 5; i++ {
		err := sp.DeleteAt(0)
		assert.NoError(t, err)
	}

	assert.Equal(t, float32(0), sp.FillFactor())


	t.Log(string(sp.values()))
	assert.Equal(t, b("\x07selam_4\x07selam_3\x07selam_2\x07selam_1\x07selam_0"), sp.values())
	assert.Equal(t, len(b("\x07selam_4\x07selam_3\x07selam_2\x07selam_1\x07selam_0")), int(sp.GetHeader().EmptyBytes))
	sp.Vacuum()

	t.Log(string(sp.values()))
	assert.Empty(t, sp.values())
	assert.Zero(t, sp.GetHeader().EmptyBytes)
	assert.Equal(t, float32(1), sp.FillFactor())
}

func TestSlottedPage_SetAt_When_New_Data_Can_Fit(t *testing.T) {
	sp := newSp()
	for i := 0; i < 5; i++ {
		sp.InsertAt(i, b(fmt.Sprintf("selam_%v", i)))
	}

	assert.Equal(t, float32(1), sp.FillFactor())

	t.Log(string(sp.values()))
	sp.SetAt(2, b("selam_9"))
	t.Log(string(sp.values()))

	assert.Equal(t, b("selam_9"), sp.GetAt(2))
	assert.Equal(t, b("\x07selam_4\x07selam_3\x07selam_9\x07selam_1\x07selam_0"), sp.values())
}

func TestSlottedPage_SetAt_When_New_Data_Cannot_Fit(t *testing.T) {
	sp := newSp()
	for i := 0; i < 5; i++ {
		sp.InsertAt(i, b(fmt.Sprintf("selam_%v", i)))
	}

	assert.Equal(t, float32(1), sp.FillFactor())

	t.Log(string(sp.values()))
	sp.SetAt(2, b("new_selam_2"))
	t.Log(string(sp.values()))

	assert.Equal(t, b("new_selam_2"), sp.GetAt(2))
	assert.Equal(t, b("\x0bnew_selam_2\x07selam_4\x07selam_3\x07selam_2\x07selam_1\x07selam_0"), sp.values())
	
	sp.Vacuum()

	t.Log(string(sp.values()))
	assert.Equal(t, b("new_selam_2"), sp.GetAt(2))
}