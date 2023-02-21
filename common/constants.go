package common

import "time"

const (
	EnableLogging = true

	// LogTimeout is the duration between each log flush operation. It is probably better to align this with disk's iops
	// rate as much as possible.
	LogTimeout = time.Millisecond * 3
)
