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

func (c *Client) SendFile(filePath string) (*files.FileMetadata, error) {
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
	results := c.processSendFile(file, fileSize)

	for result := range results {
		fmt.Println("result:", result)
	}

	fmt.Println("finished sending file")

	return nil, nil
}
