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

func (c *Client) processSendFile(file *os.File, size int64) (<-chan workers.Result, error) {
	chunkCount := size / ChunkSize
	remain := size
	if size%ChunkSize != 0 {
		chunkCount += 1
	}

	// Channel to recieve results
	res := make(chan workers.Result, chunkCount)
	// Channel to keep track of number of completed sends
	out := make(chan workers.Result, chunkCount)

	for i := int64(0); i < chunkCount; i++ {
		n := min(remain, ChunkSize)
		off := int64(i * ChunkSize)
		fileReader := io.NewSectionReader(file, off, int64(n))
		hashReader := io.NewSectionReader(file, off, int64(n))
		id, err := c.hasher.HashContent(hashReader)
		if err != nil {
			fmt.Errorf("Error occured getting checksum: %w", err)
		}
		chunkInfo := &files.ChunkInfo{
			Size:   uint32(n),
			Offset: off,
		}
		chunkMeta := files.ChunkMetaData{
			Id:        id,
			ChunkInfo: chunkInfo,
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
			func(ctx context.Context) (interface{}, error) {
				err := c.sendChunk(ctx, chunk)
				if err != nil {
					return nil, err
				}
				return chunk.Metadata, nil
			},
			res,
		)
		if err := c.writeWorkerPool.Submit(job); err != nil {
			return nil, err
		}
		remain -= n
	}

	// Keeps track of number of results recieved
	var resCount int64
	for result := range res {
		out <- result
		resCount += 1

		if resCount == chunkCount {
			close(res)
		}
	}
	close(out)
	return out, nil
}

func (c *Client) sendChunk(ctx context.Context, chunk *files.Chunk) error {
	dialTimeoutCtx, cancel := context.WithTimeout(ctx, c.config.transportConfig.dialTimeout)
	defer cancel()
	conn, err := c.connPool.Get(dialTimeoutCtx)
	if err != nil {
		return err
	}

	if err = conn.SetWriteDeadline(time.Now().Add(c.config.writeConfig.writeTimeout)); err != nil {
		conn.Close()
		return fmt.Errorf("failed to set write deadline for connection: %w", err)
	}

	done := make(chan struct{})
	defer close(done)

	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()

	length := MaxMetadataSizeInBytes + chunk.Metadata.ChunkInfo.Size + uint32(chunk.MetadataLen)
	msg := protocol.NewMessage(protocol.TypeWrite, chunk.Data, length)
	if err := c.protocol.Encode(conn, msg); err != nil {
		conn.Close()
		return err
	}

	if err := conn.SetWriteDeadline(time.Time{}); err != nil {
		fmt.Println("failed to reset write deadline, closing connection.")
		conn.Close()
		return nil
	}

	c.connPool.Put(conn)

	return nil
}
