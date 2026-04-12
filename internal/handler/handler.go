package handler

import (
	"fmt"
	"io"
	"net"

	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
)

type Handler interface {
	Handle(net.Conn) error
}

type ChunkHandler struct {
	storage  storage.Storage
	protocol protocol.Protocol
}

func NewChunkHandler(storage storage.Storage, protocol protocol.Protocol) *ChunkHandler {
	return &ChunkHandler{
		storage:  storage,
		protocol: protocol,
	}
}

func (c *ChunkHandler) Handle(conn net.Conn) error {
	for {
		m, err := c.decode(conn)
		if err != nil {
			return err
		}

		if m.Length == 0 {
			fmt.Printf("Recieved connection message with length 0. Skipping it.")
			return nil
		}

		switch m.Type {
		case protocol.TypeRead:
			fileReader, fileLen, err := c.readChunk(m.Payload)
			if err != nil {
				return err
			}

			response := &protocol.Message{
				Type:    protocol.TypeReadResponse,
				Length:  uint32(fileLen),
				Payload: fileReader,
			}

			if err := c.encode(conn, response); err != nil {
				return err
			}

		case protocol.TypeWrite:
			hardCodedKey := "testKey" // INFO: This is just a hardcoded key for testing. Implement content hashed keys as key.
			if err := c.writeChunk(hardCodedKey, m.Payload); err != nil {
				return err
			}

			fmt.Println("Successfully written the file")
		default:
			fmt.Println("Unkown message type. Skipping.")
		}
		return nil
	}
}

func (c *ChunkHandler) decode(conn net.Conn) (*protocol.Message, error) {
	msg, err := c.protocol.Decode(conn)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (c *ChunkHandler) encode(conn net.Conn, m *protocol.Message) error {
	if err := c.protocol.Encode(conn, m); err != nil {
		return err
	}

	return nil
}

func (c *ChunkHandler) readChunk(r io.Reader) (io.Reader, int64, error) {
	transformKey, err := io.ReadAll(r)
	if err != nil {
		return nil, storage.FalseLength, fmt.Errorf("failed to fetch file path transform key: %w", err)
	}

	fileReader, fileLen, err := c.storage.Read(string(transformKey))

	return fileReader, fileLen, err
}

func (c *ChunkHandler) writeChunk(key string, r io.Reader) error {
	if err := c.storage.Write(key, r); err != nil {
		return err
	}

	return nil
}
