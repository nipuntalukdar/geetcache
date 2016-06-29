package main

import (
	"hash/crc32"
	"os"
)

var VALID_TYPES map[uint8]bool = map[uint8]bool{LISTTYPE: true, PMAPTYPE: true,
	MAPTYPE: true, CTRTYPE: true}

func getPartition(key string, maxpart uint32) uint32 {
	return (crc32.ChecksumIEEE([]byte(key)) & MAX_PARTITIONS_AND) % maxpart
}

func copyslice(inp []byte) []byte {
	out := make([]byte, len(inp))
	copy(out, inp)
	return out
}

func errp(err error) {
	if err != nil {
		panic(err)
	}
}

func validType(typ uint8) bool {
	_, ok := VALID_TYPES[typ]
	return ok
}

func getFileSizeFile(file *os.File) int64 {
	filestat, err := file.Stat()
	if err != nil {
		return -1
	}
	return filestat.Size()
}
