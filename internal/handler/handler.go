package handler

import "github.com/lokeshMudhalvan/MyDFS/internal/storage"

type Handler interface {
	Handle()
}

type ChunkHandler struct {
	storage storage.Storage
}

func NewChunkHandler(storage storage.Storage) *ChunkHandler {
	return &ChunkHandler{
		storage: storage,
	}
}

// TODO: Complete the Handle method.
func (c *ChunkHandler) Handle(data []byte) error {
	return nil
}
