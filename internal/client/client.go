package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

const (
	ChunkSize = 64 * (1 << 20) // 64MB chunks
	// ChunkSize              = 2 // TEST: This is only a test value
	MaxMetadataSizeInBytes = 4 // Max Metadata length is 2^32 - 1 ~ 4GB
)

type Hasher interface {
	HashContent(io.Reader) (string, error)
}

type Client struct {
	protocol protocol.Protocol
	hasher   Hasher
	encoder  encoder.Encoder         // Encoder to serialize resulting structs
	connPool transport.TransportPool // Connection pool to connect to the chunk servers
}

func NewClient(protocol protocol.Protocol, hasher Hasher, encoder encoder.Encoder, connPool transport.TransportPool) *Client {
	return &Client{
		protocol: protocol,
		hasher:   hasher,
		encoder:  encoder,
		connPool: connPool,
	}
}

func (c *Client) SendFile(filePath string) (*files.FileMetadata, error) {
	processedChan := make(chan *files.ChunkMetaData)
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file stats: %w", err)
	}

	fileSize := fileStat.Size()
	go c.processSendFile(file, fileSize, processedChan)

	for meta := range processedChan {
		fmt.Println("finished", meta)
	}

	return nil, nil
}

func (c *Client) processSendFile(file *os.File, size int64, processedChan chan<- *files.ChunkMetaData) {
	chunkCount := size / ChunkSize
	remain := size
	if size%ChunkSize != 0 {
		chunkCount += 1
	}

	fmt.Println("This is chunkCount:", chunkCount)

	chunkChan := make(chan *files.Chunk)
	go c.sendChunk(chunkChan, processedChan)

	for i := int64(0); i < chunkCount; i++ {
		n := min(remain, ChunkSize)
		fileReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		hashReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		id, err := c.hasher.HashContent(hashReader)
		// TODO: Implement robust error handling
		if err != nil {
			fmt.Println("Error occured getting checksum:", err)
		}
		fmt.Println("This is the checksum:", id)
		chunkMeta := &files.ChunkMetaData{
			Id:   id,
			Size: uint32(n),
		}

		// Buffer to contain the metadata of the chunk
		var chunkMetaDataBuffer bytes.Buffer
		if err := c.encoder.Encode(&chunkMetaDataBuffer, chunkMeta); err != nil {
			fmt.Printf("failed to encode chunk metadata: %s", err)
		}

		metaDataLen := chunkMetaDataBuffer.Len()
		if metaDataLen > math.MaxInt32 {
			fmt.Printf("error: Metadata length is greater than allowed uint32 size")
		}
		var metaLen [4]byte
		binary.BigEndian.PutUint32(metaLen[:], uint32(metaDataLen))

		chunkData := io.MultiReader(bytes.NewBuffer(metaLen[:]), &chunkMetaDataBuffer, fileReader)

		chunk := &files.Chunk{
			Metadata:    chunkMeta,
			MetadataLen: metaDataLen,
			Data:        chunkData,
		}

		chunkChan <- chunk
		remain -= n
	}

	close(chunkChan)
}

func (c *Client) sendChunk(chunkChan <-chan *files.Chunk, processedChan chan<- *files.ChunkMetaData) error {
	// TODO: This is a temporary context. Allows to send contexts through function arguments.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for chunk := range chunkChan {
		conn, err := c.connPool.Get(ctx)
		if err != nil {
			return fmt.Errorf("failed to connect to chunk server: %w", err)
		}

		length := MaxMetadataSizeInBytes + chunk.Metadata.Size + uint32(chunk.MetadataLen)
		msg := protocol.NewMessage(protocol.TypeWrite, chunk.Data, length)
		if err := c.protocol.Encode(conn, msg); err != nil {
			return err
		}

		processedChan <- chunk.Metadata
		c.connPool.Put(conn)
	}

	close(processedChan)
	return nil
}
