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

	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	workers "github.com/lokeshMudhalvan/MyDFS/internal/wokers"
)

func (c *Client) processSendFile(file *os.File, size int64) <-chan workers.Result {
	chunkCount := size / ChunkSize
	remain := size
	if size%ChunkSize != 0 {
		chunkCount += 1
	}

	fmt.Println("This is chunkCount:", chunkCount)

	poolConfig := workers.NewPoolConfig(c.workerCount, 2*c.workerCount, c.maxRetries, c.retryDelay)
	workerPool := workers.NewWorkerPool(poolConfig, 2*c.workerCount)

	for i := int64(0); i < chunkCount; i++ {
		n := min(remain, ChunkSize)
		fileReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		hashReader := io.NewSectionReader(file, int64(i*ChunkSize), int64(n))
		id, err := c.hasher.HashContent(hashReader)
		// TODO: Implement robust error handling
		if err != nil {
			fmt.Println("Error occured getting checksum:", err)
		}
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

		job := workers.NewJob(
			func() (interface{}, error) {
				err := c.sendChunk(chunk)
				if err != nil {
					return nil, err
				}
				return chunk.Metadata, nil
			},
		)
		workerPool.Submit(job)
		remain -= n
	}

	go workerPool.Shutdown()
	return workerPool.Results()
}

func (c *Client) sendChunk(chunk *files.Chunk) error {
	// TODO: This is a temporary context. Allows to send contexts through function arguments.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := c.connPool.Get(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to chunk server: %w", err)
	}

	length := MaxMetadataSizeInBytes + chunk.Metadata.Size + uint32(chunk.MetadataLen)
	msg := protocol.NewMessage(protocol.TypeWrite, chunk.Data, length)
	if err := c.protocol.Encode(conn, msg); err != nil {
		return err
	}

	c.connPool.Put(conn)

	return nil
}
