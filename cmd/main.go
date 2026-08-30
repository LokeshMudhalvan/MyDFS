package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/client"
	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/handler"
	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/server"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
)

func main() {
	hasher := hasher.NewMD5ContentHasher()
	storage := storage.NewFileStorage(storage.HashPathTransform, 5, hasher)
	p := protocol.NewChunkTransferProtocol()
	encoder := encoder.NewGobEncoder()
	handler := handler.NewChunkHandler(storage, p, encoder)
	s := transport.NewTCPTransport(":5001", handler)
	err := s.Listen()
	if err != nil {
		fmt.Println("Error occured:", err)
	}
	wd, _ := os.Getwd()
	walDir := filepath.Join(wd, "test-wal")
	w, err := wal.InitWAL(
		walDir,
		wal.EnableFsSync(),
		wal.WithFlushInterval(5*time.Millisecond),
		wal.WithMaxSegements(3),
		wal.WithMaxSegementSize(500),
		// TEST: change this to 60 seconds
		wal.WithSnapshotInterval(10*time.Second),
	)
	if err != nil {
		fmt.Println("Failed wal initalization: ", err)
		os.Exit(1)
	}
	// TODO: change metaserver to depend on a store interface
	store := files.NewFileStore(w)
	if err = store.EnableSnapshots(); err != nil {
		fmt.Println("Failed to enable snapshots for file store: ", err)
	}
	metaServer := server.NewMetaServer(store)
	ctx := context.Background()
	client, err := client.NewClient(ctx, metaServer)

	filePath := filepath.Join(wd, "test/test1/test.mov")

	if err = client.SendFile(filePath); err != nil {
		fmt.Println("Error with client sending file:", err)
	}

	readFilePath := filepath.Join(wd, "test/test1/test-1-read-result.mov")
	// TODO: find a better way to store each file uniquely. Currently values are hardcoded
	if err = client.ReadFile("test.mov", readFilePath); err != nil {
		fmt.Println("Error with client reading file:", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	client.Close()
	s.Close()
}
