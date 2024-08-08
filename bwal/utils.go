package bwal

import "strconv"

func lsnToSegment(lsn uint64, segmentSize uint64) uint64 {
	return lsn / segmentSize
}

func lsnToSegmentStr(lsn uint64, segmentSize uint64) string {
	return strconv.Itoa(int(lsnToSegment(lsn, segmentSize)))
}

func segmentToStr(segment uint64) string {
	return strconv.Itoa(int(segment))
}

func segmentNameToSegment(segment string) (uint64, error) {
	i, err := strconv.Atoi(segment)
	return uint64(i), err
}

func lsnToSegmentOffset(lsn uint64, segmentSize uint64) uint64 {
	return lsn % segmentSize
}
