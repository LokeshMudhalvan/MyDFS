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

func (c *Client) processReadFile(fileMeta *files.FileMetadata, w io.WriterAt) (<-chan workers.Result, error) {
	infoLen := len(fileMeta.ChunkInfo)
	res := make(chan workers.Result, infoLen)
	out := make(chan workers.Result, infoLen)

	for id, chunkInfo := range fileMeta.ChunkInfo {
		job := workers.NewJob(
			func(ctx context.Context) (interface{}, error) {
				writer := adaptors.NewWriterAtAdapter(w, chunkInfo.Offset)
				err := c.readChunk(ctx, id, chunkInfo, writer)
				if err != nil {
					fmt.Println("failed to read chunk:", err)
					return nil, err
				}
				res := fmt.Sprintf("Read chunk %s", id)
				return res, nil
			},
			res,
		)

		if err := c.readWorkerPool.Submit(job); err != nil {
			return nil, err
		}
	}

	var resCount int
	for result := range res {
		out <- result
		resCount += 1

		if resCount == infoLen {
			close(res)
		}
	}
	close(out)
	return out, nil
}

func (c *Client) readChunk(ctx context.Context, id string, chunkInfo *files.ChunkInfo, w *adaptors.WriterAtAdaptper) error {
	conn, err := c.getChunkServerConn(ctx, chunkInfo.Addr)
	defer conn.Close()
	if err != nil {
		return err
	}

	if err = conn.SetReadDeadline(time.Now().Add(c.config.readConfig.readTimeout)); err != nil {
		return fmt.Errorf("failed to set read deadline: %w", err)
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
	if _, err := io.CopyN(w, msg.Payload, int64(chunkInfo.Size)); err != nil {
		return fmt.Errorf("failed to copy chunk from connection to file: %w", err)
	}

	return nil
}
