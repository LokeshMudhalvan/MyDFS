package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/metaserver"
	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
)

func main() {
	config := metaserver.MetaServerConfig{
		ListenerPort:    ":5002",
		ShutdownTimeout: 5 * time.Second,
		WALConfig:       wal.WALConfig{},
	}
	ms, err := metaserver.NewMetaServer(config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	if err := ms.Start(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer ms.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
