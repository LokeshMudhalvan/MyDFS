package files

import "io"

type ChunkMetaData struct {
	Id   string
	Size uint32 // Length of chunk bytes
}

type Chunk struct {
	Metadata    *ChunkMetaData
	MetadataLen int // Length of metadata upon converting to bytes
	Data        io.Reader
}

type FileMetadata struct {
	Size      int64
	Name      string
	ChunkInfo []ChunkMetaData
}
