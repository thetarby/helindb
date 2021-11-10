package buffer

type IReplacer interface {
	Pin(frameId int)
	Unpin(frameId int)
	ChooseVictim() (frameId int, err error)
	GetSize() int
	NumPinnedPages() int
}
