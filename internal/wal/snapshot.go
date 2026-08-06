package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

	return nil
}

func (w *WAL) performCleanup() error {
	return nil
}
