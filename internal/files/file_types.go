package files

import "io"

type ChunkInfo struct {
	Size   uint32
	Offset int64
}

type ChunkMetaData struct {
	Id        string
	ChunkInfo ChunkInfo
}

type Chunk struct {
	Metadata    ChunkMetaData
	MetadataLen int // Length of metadata upon converting to bytes
	Data        io.Reader
}

type FileMetadata struct {
	Size      int64
	Name      string
	ChunkInfo map[string]ChunkInfo
}
