package handler

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
)

var ErrCheckSumNotMatching = errors.New("checksum did not match")

type Handler interface {
	Handle(net.Conn) error
}

type ChunkHandler struct {
	storage  storage.Storage
	protocol protocol.Protocol
	encoder  encoder.Encoder
}

func NewChunkHandler(storage storage.Storage, protocol protocol.Protocol, encoder encoder.Encoder) *ChunkHandler {
	return &ChunkHandler{
		storage:  storage,
		protocol: protocol,
		encoder:  encoder,
	}
}

func (c *ChunkHandler) Handle(conn net.Conn) error {
	for {
		m, err := c.decode(conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if m.Length == 0 {
			fmt.Printf("Recieved connection message with length 0. Skipping it.")
			return nil
		}

		switch m.Type {
		case protocol.TypePing:
			if err := c.handlePing(m.Payload, conn); err != nil {
				return err
			}
		case protocol.TypeRead:
			if err := c.handleRead(m.Payload, conn); err != nil {
				return err
			}
		case protocol.TypeWrite:
			if err := c.handleWrite(m.Payload); err != nil {
				return err
			}

			fmt.Println("Successfully written the file")
		default:
			fmt.Println("Unkown message type. Skipping.")
		}
	}
}

func (c *ChunkHandler) handleWrite(r io.Reader) error {
	chunkMetaDataReader, err := c.getChunkMetadataReader(r)
	if err != nil {
		return err
	}
	chunkMetaData, err := c.readChunkMetadata(chunkMetaDataReader)
	if err != nil {
		return err
	}
	fileMetadata, err := c.writeChunk(chunkMetaData.Id, r)
	if err != nil {
		return err
	}

	if result := c.verifyCheckSum(chunkMetaData.Id, fileMetadata.ContentHash); !result {
		return ErrCheckSumNotMatching
	}

	if err := c.renameFile(fileMetadata.FullPath.GetFilePath()); err != nil {
		return fmt.Errorf("failed to rename file: %w", err)
	}

	return nil
}

func (c *ChunkHandler) handleRead(r io.Reader, conn net.Conn) error {
	// TODO: Change the readChunk method to give a reader with chunk meta data as well
	fileReader, fileLen, err := c.readChunk(r)
	if err != nil {
		return err
	}
	response := protocol.NewMessage(protocol.TypeReadResponse, fileReader, uint32(fileLen))

	if err := c.encode(conn, response); err != nil {
		return err
	}

	return nil
}

func (c *ChunkHandler) handlePing(r io.Reader, conn net.Conn) error {
	msg, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("error reading ping message: %w", err)
	}
	response := protocol.NewMessage(protocol.TypePingResponse, bytes.NewBuffer(msg), uint32(len(msg)))

	if err := c.protocol.Encode(conn, response); err != nil {
		return err
	}

	return nil
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

func (c *ChunkHandler) writeChunk(key string, r io.Reader) (storage.FileMetaData, error) {
	fileMetadata, err := c.storage.Write(key, r)
	if err != nil {
		return storage.FileMetaData{}, err
	}

	return fileMetadata, nil
}

func (c *ChunkHandler) getChunkMetadataReader(r io.Reader) (io.Reader, error) {
	var metaDataLen [4]byte
	if _, err := io.ReadFull(r, metaDataLen[:]); err != nil {
		return nil, fmt.Errorf("failed to read metadata length: %w", err)
	}

	metaLen := binary.BigEndian.Uint32(metaDataLen[:])
	reader := io.LimitReader(r, int64(metaLen))
	return reader, nil
}

func (c *ChunkHandler) readChunkMetadata(r io.Reader) (files.ChunkMetaData, error) {
	var metadata files.ChunkMetaData
	if err := c.encoder.Decode(r, &metadata); err != nil {
		return files.ChunkMetaData{}, fmt.Errorf("failed to decode chunk metadata: %w", err)
	}
	return metadata, nil
}

func (c *ChunkHandler) verifyCheckSum(chunkCheckSum string, writtenCheckSum string) bool {
	if chunkCheckSum != writtenCheckSum {
		return false
	}

	fmt.Println("Verified checksum")
	return true
}

func (c *ChunkHandler) renameFile(oldPath string) error {
	return c.storage.RenameFile(oldPath)
}
