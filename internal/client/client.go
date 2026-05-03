package client

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"os"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
)

const (
	ChunkSize = 64 * (1 << 20) // 64MB chunks
	// ChunkSize              = 2 // TEST: This is only a test value
	MaxMetadataSizeInBytes = 4 // Max Metadata length is 2^32 - 1 ~ 4GB
)

type ChunkMetaData struct {
	Id   string
	Size uint32 // Length of chunk bytes
}

type Chunk struct {
	metadata    *ChunkMetaData
	metadataLen int // Length of metadata upon converting to bytes
	data        io.Reader
}

type FileMetadata struct {
	size      int64
	name      string
	chunkInfo []ChunkMetaData
}

type Client struct {
	addr     string
	protocol protocol.Protocol
	hasher   hasher.Hasher
	encoder  encoder.Encoder // Encoder to serialize resulting structs
}

func NewClient(addr string, protocol protocol.Protocol, hasher hasher.Hasher, encoder encoder.Encoder) *Client {
	return &Client{
		addr:     addr,
		protocol: protocol,
		hasher:   hasher,
		encoder:  encoder,
	}
}

func (c *Client) SendFile(filePath string) (*FileMetadata, error) {
	processedChan := make(chan *ChunkMetaData)
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
	go c.processSendFile(file, int(fileSize), processedChan)

	for meta := range processedChan {
		fmt.Println("finished", meta)
	}

	return nil, nil
}

func (c *Client) processSendFile(file *os.File, size int, processedChan chan<- *ChunkMetaData) {
	chunkCount := size / ChunkSize
	remain := size
	if size%ChunkSize != 0 {
		chunkCount += 1
	}

	fmt.Println("This is chunkCount:", chunkCount)

	chunkChan := make(chan *Chunk)
	go c.sendChunk(chunkChan, processedChan)

	for i := 0; i < chunkCount; i++ {
		n := min(remain, ChunkSize)
		fileReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		hashReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		id, err := c.hasher(hashReader)
		// TODO: Implement robust error handling
		if err != nil {
			fmt.Println("Error occured getting checksum:", err)
		}
		fmt.Println("This is the checksum:", id)
		chunkMeta := &ChunkMetaData{
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

		chunk := &Chunk{
			metadata:    chunkMeta,
			metadataLen: metaDataLen,
			data:        chunkData,
		}

		fmt.Println("Sending to chunkChan")
		chunkChan <- chunk
	}

	close(chunkChan)
}

func (c *Client) sendChunk(chunkChan <-chan *Chunk, processedChan chan<- *ChunkMetaData) error {
	for chunk := range chunkChan {
		// TODO: Use a tcp connection pool
		conn, err := net.Dial("tcp", c.addr)
		if err != nil {
			return fmt.Errorf("failed to connect to chunk server: %w", err)
		}

		// TODO: Add a Message constructor
		msg := &protocol.Message{
			Type:    protocol.TypeWrite,
			Length:  MaxMetadataSizeInBytes + chunk.metadata.Size + uint32(chunk.metadataLen),
			Payload: chunk.data,
		}
		if err := c.protocol.Encode(conn, msg); err != nil {
			return err
		}

		fmt.Println("Sending to processedChan")
		processedChan <- chunk.metadata
	}

	close(processedChan)
	return nil
}
