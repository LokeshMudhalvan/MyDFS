package files

import (
	"bufio"
	"fmt"
	"io"

	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
	"google.golang.org/protobuf/proto"
)

func (f *FileStore) Snapshot(w io.Writer) error {
	bw := bufio.NewWriter(w)
	clone := make(map[string]*FileMetadata)

	f.lock.RLock()
	for name, meta := range f.Files {
		clone[name] = meta
	}
	f.lock.RUnlock()

	for _, meta := range clone {
		metadata, err := proto.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata")
		}

		if _, err := bw.Write(metadata); err != nil {
			return fmt.Errorf("failed to write metadata to snapshot")
		}
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush snapshot data")
	}

	return nil
}

func (f *FileStore) Restore(r io.Reader) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	return nil
}

func (f *FileStore) Apply(w *wal.WAL_Entry) error {
	return nil
}
