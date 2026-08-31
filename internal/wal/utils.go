package wal

import (
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type CRCVerifiable interface {
	GetCRC() uint32
	GetData() []byte
}

func (w *WAL) getSegmentNoFromFileName(name string) (uint64, error) {
	nameSplit := strings.Split(name, "-")
	segNo, err := strconv.ParseUint(nameSplit[len(nameSplit)-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to convert file segment number to integer: %w", err)
	}
	return segNo, nil
}

func (w *WAL) getPreviousSegmentNo(f *os.File) (uint64, error) {
	name := filepath.Base(f.Name())
	segNo, err := w.getSegmentNoFromFileName(name)
	if err != nil {
		return 0, err
	}
	return segNo - 1, nil
}

func (w *WAL) generateLogFilePath(segNo uint64) string {
	return filepath.Join(w.dir + "/" + WalLogPrefix + strconv.Itoa(int(segNo)))
}

func (w *WAL) verifyCRC(entry CRCVerifiable) bool {
	return entry.GetCRC() == crc32.ChecksumIEEE(entry.GetData())
}

func (w *WAL) generateCRC(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}
