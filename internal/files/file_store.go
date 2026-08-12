package files

import (
	"bufio"
	"fmt"
	"io"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
	"google.golang.org/protobuf/proto"
)

func NewFileStore(walDir string) (*FileStore, error) {
	store := &FileStore{
		files: make(map[string]*FileMetadata),
	}
	wal, err := wal.InitWAL(
		walDir,
		store,
		wal.EnableFsSync(),
		wal.WithFlushInterval(5*time.Millisecond),
		wal.WithMaxSegements(3),
		wal.WithMaxSegementSize(500),
		// TEST: change this to 60 seconds
		wal.WithSnapshotInterval(10*time.Second),
	)
	if err != nil {
		return nil, err
	}
	store.wal = wal

	return store, nil
}

func (f *FileStore) AddFileMetadata(fMeta *FileMetadata) error {
	meta, err := proto.Marshal(fMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}
	err = f.wal.AppendEntry(meta, false)
	if err != nil {
		return nil
	}
	f.files[fMeta.GetName()] = fMeta
	return nil
}

func (f *FileStore) LookupFileMetadata(key string) *FileMetadata {
	if fMeta, ok := f.files[key]; ok {
		return fMeta
	}

	return nil
}

func (f *FileStore) DeleteFileMetadata(key string) error {
	fMeta, ok := f.files[key]
	if !ok {
		return nil
	}
	meta, err := proto.Marshal(fMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}
	err = f.wal.AppendEntry(meta, true)
	if err != nil {
		return nil
	}

	delete(f.files, key)
	return nil
}

func (f *FileStore) Snapshot(w io.Writer) error {
	bw := bufio.NewWriter(w)
	clone := make(map[string]*FileMetadata)

	f.lock.RLock()
	for name, meta := range f.files {
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
