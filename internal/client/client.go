package client

import (
	"fmt"
	"io"
	"net"
	"os"

	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
)

const (
	// ChunkSize = 64 * (1 << 20) // 64MB chunks
	ChunkSize = 2 // TEST: This is only a test value
)

type ChunkMetaData struct {
	Id   string
	Size uint32
}

type Chunk struct {
	metadata *ChunkMetaData
	data     io.Reader
}

// TODO: Mechanism to get unique name for file. Maybe hash all the chunkIDs combined together?
type FileMetadata struct {
	size      int64
	name      string
	chunkInfo []ChunkMetaData
}

type Client struct {
	addr     string
	protocol protocol.Protocol
	hasher   hasher.Hasher
}

func NewClient(addr string, protocol protocol.Protocol, hasher hasher.Hasher) *Client {
	return &Client{
		addr:     addr,
		protocol: protocol,
		hasher:   hasher,
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
		reader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		hashReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		id, err := c.hasher(hashReader)
		// TODO: Implement robust error handling
		if err != nil {
			fmt.Println("Error occured getting checksum:", err)
		}
		fmt.Println("This is the checksum:", id)
		chunkMeta := &ChunkMetaData{
			// INFO: The id is just for testing. The ID needs to be the checksum.
			// TODO: Implement hashing for the checksum.
			Id:   id,
			Size: uint32(n),
		}

		chunk := &Chunk{
			metadata: chunkMeta,
			data:     reader,
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
			Length:  chunk.metadata.Size,
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
