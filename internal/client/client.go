package client

import (
	"fmt"
	"io"
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
	protocol    protocol.Protocol
	hasher      Hasher
	encoder     encoder.Encoder         // Encoder to serialize resulting structs
	connPool    transport.TransportPool // Connection pool to connect to the chunk servers
	workerCount int
	maxRetries  int
	retryDelay  time.Duration
}

func NewClient(
	protocol protocol.Protocol,
	hasher Hasher,
	encoder encoder.Encoder,
	connPool transport.TransportPool,
	workerCount int,
	maxRetries int,
	retryDelay time.Duration,
) *Client {
	return &Client{
		protocol:    protocol,
		hasher:      hasher,
		encoder:     encoder,
		connPool:    connPool,
		workerCount: workerCount,
		maxRetries:  maxRetries,
		retryDelay:  retryDelay,
	}
}

func (c *Client) SendFile(filePath string) (files.FileMetadata, error) {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return files.FileMetadata{}, fmt.Errorf("failed to open file: %w", err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		return files.FileMetadata{}, fmt.Errorf("failed to get file stats: %w", err)
	}

	fileSize := fileStat.Size()
	results := c.processSendFile(file, fileSize)
	chunkInfo := make(map[string]files.ChunkInfo)

	// TODO: Handle error sent through result
	for result := range results {
		chunkMeta, ok := result.Output.(files.ChunkMetaData)
		if !ok {
			return files.FileMetadata{}, fmt.Errorf("failed to type cast result output to chunk meta data")
		}
		chunkInfo[chunkMeta.Id] = chunkMeta.ChunkInfo
	}

	return files.FileMetadata{
		Size:      fileSize,
		Name:      fileStat.Name(),
		ChunkInfo: chunkInfo,
	}, nil
}

// TODO: Implement a way to store File Metadata on disk and load it into memory. Also handle case if the file path does not exist already
func (c *Client) ReadFile(fileMeta files.FileMetadata, filePath string) error {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_TRUNC, os.ModePerm)
	defer file.Close()
	if err != nil {
		return err
	}

	results := c.processReadFile(fileMeta, file)
	// TODO: Handle error sent through result
	for result := range results {
		fmt.Println(result.Output)
	}

	return nil
}
