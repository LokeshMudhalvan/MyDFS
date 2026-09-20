package metaserver

import (
	"fmt"

	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

type MetaServer struct {
	transport transport.Transport
}

func (m *MetaServer) Start() error {
	if err := m.transport.Listen(); err != nil {
		return err
	}
	fmt.Println("meta server started")
	return nil
}

func (m *MetaServer) Stop() {
	m.transport.Close()
	fmt.Println("meta server closed")
}
