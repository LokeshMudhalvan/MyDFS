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
	"github.com/lokeshMudhalvan/MyDFS/internal/handler"
	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

func main() {
	hasher := hasher.NewMD5ContentHasher()
	storage := storage.NewFileStorage(storage.HashPathTransform, 5, hasher)
	p := protocol.NewChunkTransferProtocol()
	encoder := encoder.NewGobEncoder()
	handler := handler.NewChunkHandler(storage, p, encoder)
	s := transport.NewTCPTransport(":5001", handler)
	ctx := context.Background()
	err := s.Listen()
	if err != nil {
		fmt.Println("Error occured:", err)
	}
	connPool, err := transport.NewTCPPool(ctx, p, ":5001", 10, 5*time.Second)
	if err != nil {
		fmt.Println("An error occured while creating TCP Pool. Exiting...", err)
		os.Exit(1)
	}
	client := client.NewClient(p, hasher, encoder, connPool, uint8(5))

	wd, _ := os.Getwd()
	filePath := filepath.Join(wd, "test/test1/test.mov")
	_, err = client.SendFile(filePath)
	fmt.Println("sending file")
	if err != nil {
		fmt.Println("Error with client sending file:", err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	connPool.ClosePool()
	fmt.Println("Shutting down TCP listener")
	s.Close()
}
