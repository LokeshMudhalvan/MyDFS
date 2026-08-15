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

func (w *WAL) generateSnapshotFileName() string {
	return w.dir + "/" + SnapshotFile
}

func (w *WAL) takeSnapshot() error {
	w.mu.Lock()
	seqNo := w.lastSequenceNo
	segNo := w.lastSegmentNo
	w.mu.Unlock()

	fPath := w.generateSnapshotFileName() + ".tmp"
	f, err := os.OpenFile(fPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	defer f.Close()
	if err != nil {
		return fmt.Errorf("failed to open new snapshot file: %w", err)
	}
	bw := bufio.NewWriter(f)

	if err = binary.Write(bw, binary.BigEndian, seqNo); err != nil {
		return fmt.Errorf("failed to write last sequence number: %w", err)
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

// TODO: this uses the snapshottable interface methods such as Restore and Apply to build back the state
// returns the last sequence number of the last snapshot during restoration
func (w *WAL) restore() (uint64, error) {
	var seqNo uint64

	fName := w.generateSnapshotFileName()
	f, err := os.Open(fName)
	defer f.Close()
	if err != nil {
		if os.IsNotExist(err) {
			return seqNo, ErrNoSnapshotFile
		}
		return seqNo, fmt.Errorf("failed to open snapshot file: %w", err)
	}

	if err = binary.Read(f, binary.BigEndian, &seqNo); err != nil {
		return seqNo, fmt.Errorf("failed to read ")
	}

	if err = w.snapshotable.Restore(f); err != nil {
		return seqNo, err
	}

	entries, err := w.ReadAllEntries()
	if err != nil {
		return seqNo, err
	}
	for _, entry := range entries {
		if err = w.snapshotable.Apply(entry); err != nil {
			return seqNo, err
		}
	}

	return seqNo, nil
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
