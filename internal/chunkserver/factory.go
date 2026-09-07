package chunkserver

import (
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

type ChunkServerConfig struct {
	ListenerPort    string
	ShutdownTimeout time.Duration
	FileDepth       int
}

func NewChunkServer(c ChunkServerConfig) *ChunkServer {
	s := storage.NewFileStorage(
		storage.DefaultPathTransform,
		c.FileDepth,
		hasher.NewMD5ContentHasher(),
	)
	handler := NewChunkHandler(
		s,
		protocol.NewChunkTransferProtocol(),
		encoder.NewGobEncoder(),
	)
	t := transport.NewTCPTransport(
		c.ListenerPort,
		handler,
		transport.WithShutdownTimeout(c.ShutdownTimeout),
	)
	return &ChunkServer{
		transport: t,
	}
}
