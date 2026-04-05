package handler

import (
	"fmt"
	"io"

	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
)

type Handler interface {
	Handle(io.Writer, *protocol.Message) error
}

type ChunkHandler struct {
	storage storage.Storage
}

func NewChunkHandler(storage storage.Storage) *ChunkHandler {
	return &ChunkHandler{
		storage: storage,
	}
}

func (c *ChunkHandler) Handle(w io.Writer, m *protocol.Message) error {
	if m.Length == 0 {
		fmt.Printf("Recieved connection message with length 0. Skipping it.")
		return nil
	}

	switch m.Type {
	case protocol.TypeRead:
		transformKey, err := io.ReadAll(m.Payload)
		if err != nil {
			return fmt.Errorf("failed to fetch file path transform key: %w", err)
		}

		fileReader, fileLen, err := c.storage.Read(string(transformKey))
		if err != nil {
			return err
		}

		response := &protocol.Message{
			Type:    protocol.TypeReadResponse,
			Length:  uint32(fileLen),
			Payload: fileReader,
		}

		if err := protocol.Encode(w, response); err != nil {
			return err
		}
	case protocol.TypeWrite:
		hardCodedKey := "testKey" // INFO: This is just a hardcoded key for testing. Implement content hashed keys as key.
		if err := c.storage.Write(hardCodedKey, m.Payload); err != nil {
			return err
		}
		fmt.Println("Successfully written the file")
	default:
		fmt.Println("Unkown message type. Skipping.")
	}
	return nil
}
