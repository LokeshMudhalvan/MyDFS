package files

import (
	"bufio"
	"encoding/binary"
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

func (f *FileStore) AddFileMetadata(fMeta *FileMetadata, addToWAL bool) error {
	meta, err := proto.Marshal(fMeta)
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}

	if addToWAL {
		if err = f.wal.AppendEntry(meta, false); err != nil {
			return nil
		}
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
		length := len(metadata)
		if err = binary.Write(bw, binary.BigEndian, uint32(length)); err != nil {
			return fmt.Errorf("failed to write metadata length to snapshot")
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
	for {
		var length uint32
		var meta *FileMetadata

		if err := binary.Read(r, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				return nil
			} else {
				return fmt.Errorf("failed to read length during restore: %w", err)
			}
		}

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("failed to read data during restore: %w", err)
		}

		if err := proto.Unmarshal(data, meta); err != nil {
			return fmt.Errorf("failed to unmarshal data during restore: %w", err)
		}

		if err := f.AddFileMetadata(meta, false); err != nil {
			return err
		}
	}
}

func (f *FileStore) Apply(w *wal.WAL_Entry) error {
	var meta *FileMetadata

	data := w.GetData()

	if err := proto.Unmarshal(data, meta); err != nil {
		return fmt.Errorf("failed to unmarshal data during restore: %w", err)
	}

	if w.GetIsDelete() {
		if err := f.DeleteFileMetadata(meta.GetName()); err != nil {
			return err
		}
	} else {
		if err := f.AddFileMetadata(meta, false); err != nil {
			return err
		}
	}

	return nil
}
