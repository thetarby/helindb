package pages

import "encoding/binary"

type LSN uint64

const ZeroLSN = 0

func PutLSN(dest []byte, r LSN) {
	binary.BigEndian.PutUint64(dest, uint64(r))
}

func ReadLSN(src []byte) LSN {
	return LSN(binary.BigEndian.Uint64(src))
}
