package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Snapshotable interface {
	Snapshot(io.Writer) error
	Restore(io.Reader) error
	Apply(*WAL_Entry) error
}

func (w *WAL) takeSnapshot() error {
	w.mu.Lock()
	seqNo := w.lastSequenceNo
	segNo := w.lastSegmentNo
	w.mu.Unlock()

	fPath := w.dir + SnapshotFile + ".tmp"
	f, err := os.OpenFile(fPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to open new snapshot file: %w", err)
	}
	bw := bufio.NewWriter(f)

	if err = binary.Write(bw, binary.BigEndian, uint32(seqNo)); err != nil {
		return fmt.Errorf("failed to write last segment number: %w", err)
	}

	if err = bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush data to snapshot file: %w", err)
	}

	if err = w.snapshotable.Snapshot(f); err != nil {
		return err
	}

	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync snapshot file: %w", err)
	}

	if err = os.Rename(fPath, strings.TrimSuffix(fPath, filepath.Ext(fPath))); err != nil {
		return fmt.Errorf("failed to rename snapshot file: %w", err)
	}

	w.lastSnapshot = seqNo
	if err = w.performCleanup(segNo); err != nil {
		return err
	}

	return nil
}

// Deletes all the segements before the segment in which the snapshot was taken
func (w *WAL) performCleanup(segNo uint64) error {
	files, err := w.listAllWALLogFiles()
	if err != nil {
		return err
	}

	for _, f := range files {
		nameSplit := strings.Split(f, "-")
		extractedSegNo := nameSplit[len(nameSplit)-1]
		fSegNo, err := strconv.Atoi(extractedSegNo)
		if err != nil {
			return fmt.Errorf("failed to convert segment number to int: %w", err)
		}
		if uint64(fSegNo) < segNo {
			w.mu.Lock()
			err = w.removeLogFile(uint64(fSegNo))
			w.mu.Unlock()
			if err != nil {
				return err
			}
		}
	}

	return nil
}
