package files

import (
	"io"
	"sync"

	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
)

type ChunkMetaData struct {
	Id        string
	ChunkInfo *ChunkInfo
}

type Chunk struct {
	Metadata    ChunkMetaData
	MetadataLen int // Length of metadata upon converting to bytes
	Data        io.Reader
}

type FileStore struct {
	files map[string]*FileMetadata
	lock  sync.RWMutex
	wal   *wal.WAL
}
