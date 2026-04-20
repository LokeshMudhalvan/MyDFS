package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/lokeshMudhalvan/MyDFS/internal/client"
	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/handler"
	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

func main() {
	storage := storage.NewFileStorage(storage.HashPathTransform, 5, hasher.MD5ContentHash)
	protocol := protocol.NewChunkTransferProtocol()
	encoder := encoder.NewGobEncoder()
	handler := handler.NewChunkHandler(storage, protocol, encoder)
	s := transport.NewTCPTransport(":5001", handler)
	client := client.NewClient(":5001", protocol, hasher.MD5ContentHash, encoder)
	err := s.Listen()
	if err != nil {
		fmt.Println("Error occured:", err)
	}

	wd, _ := os.Getwd()
	filePath := filepath.Join(wd, "test/test1/test-1.txt")
	_, err = client.SendFile(filePath)
	fmt.Println("sending file")
	if err != nil {
		fmt.Println("Error with client sending file:", err)
	}
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down TCP listener")
	s.Close()
}
