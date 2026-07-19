package files

import "io"

type ChunkMetaData struct {
	Id        string
	ChunkInfo *ChunkInfo
}

type Chunk struct {
	Metadata    ChunkMetaData
	MetadataLen int // Length of metadata upon converting to bytes
	Data        io.Reader
}
