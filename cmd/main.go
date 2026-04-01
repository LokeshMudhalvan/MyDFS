package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
)

func main() {
	s := transport.NewTCPTransport(":5001")
	err := s.Listen()
	if err != nil {
		fmt.Println("Error occured:", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	fmt.Println("Shutting down TCP listener")
	s.Close()
}
