package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/adaptors"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	workers "github.com/lokeshMudhalvan/MyDFS/internal/wokers"
)

func (c *Client) processReadFile(fileMeta files.FileMetadata, w io.WriterAt) <-chan workers.Result {
	poolConfig := workers.NewPoolConfig(c.workerCount, 2*c.workerCount, c.maxRetries, c.retryDelay)
	workerPool := workers.NewWorkerPool(poolConfig, 2*c.workerCount)

	for id, chunkInfo := range fileMeta.ChunkInfo {
		job := workers.NewJob(
			func() (interface{}, error) {
				writer := adaptors.NewWriterAtAdapter(w, chunkInfo.Offset)
				err := c.readChunk(id, chunkInfo.Size, writer)
				if err != nil {
					fmt.Println("failed to read chunk:", err)
					return nil, err
				}
				res := fmt.Sprintf("Read chunk %s", id)
				return res, nil
			},
		)

		workerPool.Submit(job)
	}
	go workerPool.Shutdown()
	return workerPool.Results()
}

func (c *Client) readChunk(id string, size uint32, w *adaptors.WriterAtAdaptper) error {
	// TODO: This is a temporary context. Allow to send contexts through function arguments
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := c.connPool.Get(ctx)
	if err != nil {
		return err
	}

	buf := bytes.NewBufferString(id)
	msg := protocol.NewMessage(protocol.TypeRead, buf, uint32(len(id)))
	if err := c.protocol.Encode(conn, msg); err != nil {
		return err
	}

	msg, err = c.protocol.Decode(conn)
	if err != nil {
		return err
	}
	if _, err := io.CopyN(w, msg.Payload, int64(size)); err != nil {
		return fmt.Errorf("failed to copy chunk from connection to file: %w", err)
	}
	c.connPool.Put(conn)

	return nil
}
