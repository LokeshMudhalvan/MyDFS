package files

import (
	"io"

	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
)

func (f *FileStore) Snapshot(w io.Writer) error {
	return nil
}

func (f *FileStore) Restore(r io.Reader) error {
	return nil
}

func (f *FileStore) Apply(w *wal.WAL_Entry) error {
	return nil
}
