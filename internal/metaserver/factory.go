package metaserver

import (
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
)

type MetaServerConfig struct {
	ListenerPort    string
	ShutdownTimeout time.Duration
	WALConfig       wal.WALConfig
}

func NewMetaServer(m MetaServerConfig) (*MetaServer, error) {
	w, err := wal.InitWALwithConfig(m.WALConfig)
	if err != nil {
		return nil, err
	}
	mStore := files.NewFileStore(w)
	handler := NewMetaServerHandler(
		mStore,
		protocol.NewMessageTransferProtocol(),
		encoder.NewGobEncoder(),
	)
	t := transport.NewTCPTransport(
		m.ListenerPort,
		handler,
		transport.WithShutdownTimeout(m.ShutdownTimeout),
	)
	return &MetaServer{
		transport: t,
	}, nil
}
