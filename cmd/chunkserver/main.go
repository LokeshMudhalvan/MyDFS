package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/chunkserver"
)

func main() {
	config := chunkserver.ChunkServerConfig{
		FileDepth:       5,
		ListenerPort:    ":5001",
		ShutdownTimeout: 5 * time.Second,
	}

	cs := chunkserver.NewChunkServer(config)
	if err := cs.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer cs.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
