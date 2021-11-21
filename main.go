package main

import (
	"encoding/json"
	"helin/buffer"
	"helin/disk"
)

type demostruct struct {
	Num int
	Val string
}

//func main() {
//	d, _ := NewDiskManager("sa")
//	page := d.NewPage()
//	fmt.Println("lalalla")
//	fmt.Println(fmt.Sprintf("Writing to page %d", page))
//	x := demostruct{Num: 45, Val: "selam"}
//	json, _ := json.Marshal(x)
//	var data [4096]byte
//
//	copy(data[:], json)
//
//	d.WritePage((data[:]), page)
//	read, _ := d.ReadPage(page)
//	fmt.Print(string(read[:]))
//}

func main() {
	buff := buffer.NewBufferPool("sa", 32)

	for i := 0; i < 50; i++ {
		x := demostruct{Num: i, Val: "selam"}
		json, _ := json.Marshal(x)
		var data [4096]byte
		copy(data[:], json)

		p, err := buff.NewPage()
		println(p.GetPageId())
		if err != nil {
			println(err.Error())
		}

		data[4095] = byte('\n')
		p.(*disk.RawPage).Data = data[:]

		buff.Unpin(p.GetPageId(), true)
	}

	buff.FlushAll()
	//for _, frame := range buff.frames {
	//	buff.Flush(frame.GetPageId())
	//}
}
