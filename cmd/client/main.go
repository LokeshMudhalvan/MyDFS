package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/lokeshMudhalvan/MyDFS/internal/client"
)

func main() {
	wd, _ := os.Getwd()
	ctx := context.Background()
	client, err := client.NewClient(ctx)
	if err != nil {
		fmt.Println("failed to initialize client:", err)
	}

	filePath := filepath.Join(wd, "test/test1/test.mov")

	if err = client.SendFile(ctx, filePath); err != nil {
		fmt.Println("Error with client sending file:", err)
	}

	readFilePath := filepath.Join(wd, "test/test1/test-1-read-result.mov")
	// TODO: Client recieves the file to read from the user
	if err = client.ReadFile(ctx, "test.mov", readFilePath); err != nil {
		fmt.Println("Error with client reading file:", err)
	}

	defer client.Close()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
