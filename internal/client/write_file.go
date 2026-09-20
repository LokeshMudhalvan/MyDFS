package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	workers "github.com/lokeshMudhalvan/MyDFS/internal/wokers"
)

func (c *Client) chunkFiles(f *os.File, size int64) ([]*files.Chunk, error) {
	var chunks []*files.Chunk
	chunkCount := size / ChunkSize
	remain := size
	if size%ChunkSize != 0 {
		chunkCount += 1
	}

	for i := int64(0); i < chunkCount; i++ {
		n := min(remain, ChunkSize)
		off := int64(i * ChunkSize)
		fileReader := io.NewSectionReader(f, off, int64(n))
		hashReader := io.NewSectionReader(f, off, int64(n))
		id, err := c.hasher.HashContent(hashReader)
		if err != nil {
			return nil, fmt.Errorf("Error occured getting checksum: %w", err)
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

		chunks = append(chunks, chunk)
		remain -= n
	}

	return chunks, nil
}

func (c *Client) sendChunks(chunks []*files.Chunk) (<-chan workers.Result, error) {
	chunkCount := int64(len(chunks))
	// Channel to recieve results
	res := make(chan workers.Result, chunkCount)
	// Channel to keep track of number of completed sends
	out := make(chan workers.Result, chunkCount)

	for _, chunk := range chunks {
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
	conn, err := c.getChunkServerConn(ctx, chunk.Metadata.ChunkInfo.Addr)
	defer conn.Close()
	if err != nil {
		return err
	}

	length := MaxMetadataSizeInBytes + chunk.Metadata.ChunkInfo.Size + uint32(chunk.MetadataLen)
	msg := protocol.NewMessage(protocol.TypeWrite, chunk.Data, length)
	if err := c.protocol.Encode(conn, msg); err != nil {
		return err
	}

	return nil
}
