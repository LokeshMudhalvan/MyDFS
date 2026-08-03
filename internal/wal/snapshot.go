package wal

import "io"

type Snapshotable interface {
	Snapshot(io.Writer) error
	Restore(io.Reader) error
	Apply(*WAL_Entry) error
}

func (w *WAL) takeSnapshot() error {
	return nil
}
