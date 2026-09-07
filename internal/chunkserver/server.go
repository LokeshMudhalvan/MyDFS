package chunkserver

import (
	"fmt"

	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

type ChunkServer struct {
	transport transport.Transport
}

func (c *ChunkServer) Start() error {
	if err := c.transport.Listen(); err != nil {
		return err
	}
	fmt.Println("chunk server started")
	return nil
}

func (c *ChunkServer) Stop() {
	c.transport.Close()
	fmt.Println("chunk server closed")
}
