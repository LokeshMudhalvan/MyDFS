package wal

import (
	"hash/crc32"
	"path/filepath"
	"strconv"
)

func (w *WAL) generateLogFilePath(segNo uint64) string {
	return filepath.Join(w.dir + "/" + WalLogPrefix + strconv.Itoa(int(segNo)))
}

func (w *WAL) verifyCRC(entry *WAL_Entry) bool {
	return entry.GetCRC() == crc32.ChecksumIEEE(entry.GetData())
}

func (w *WAL) generateCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
