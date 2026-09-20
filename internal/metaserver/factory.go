package metaserver

import (
	"time"

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
	if err = w.EnableSnapshots(mStore); err != nil {
		return nil, err
	}

	handler := NewMetaServerHandler(
		mStore,
		protocol.NewMessageTransferProtocol(),
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
