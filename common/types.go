package common

type Key interface {
	Less(than Key) bool
}
