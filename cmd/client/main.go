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
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/server"
	"github.com/lokeshMudhalvan/MyDFS/internal/wal"
)

func main() {
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
	if err != nil {
		fmt.Println("failed to initialize client:", err)
	}

	filePath := filepath.Join(wd, "test/test1/test.mov")

	if err = client.SendFile(filePath); err != nil {
		fmt.Println("Error with client sending file:", err)
	}

	readFilePath := filepath.Join(wd, "test/test1/test-1-read-result.mov")
	// TODO: Client recieves the file to read from the user
	if err = client.ReadFile("test.mov", readFilePath); err != nil {
		fmt.Println("Error with client reading file:", err)
	}

	defer client.Close()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
