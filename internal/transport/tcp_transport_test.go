package transport

import (
	"bytes"
	"net"
	"os"
	"testing"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
)

func TestTCPListen(t *testing.T) {
	s := NewTCPTransport(":5001")
	if err := s.Listen(); err != nil {
		t.Error("failed to establish TCP connection", err)
	}

	testFile, err := os.Open("../../test/test1/test-1.txt")
	if err != nil {
		t.Error("failed to open test file", err)
	}

	fileStats, err := testFile.Stat()
	if err != nil {
		t.Error("failed to get file stats", err)
	}
	fileSize := fileStats.Size()
	writeRequestMsg := &protocol.Message{
		Type:    protocol.TypeWrite,
		Length:  uint32(fileSize),
		Payload: testFile,
	}

	conn, err := net.Dial("tcp", ":5001")
	if err != nil {
		t.Error("failed to establish connection with TCP server", err)
	}

	if err := protocol.Encode(conn, writeRequestMsg); err != nil {
		t.Error(err)
	}

	time.Sleep(3 * time.Second)

	readKey := []byte("testKey")
	keyReader := bytes.NewReader(readKey)

	readRequestMsg := &protocol.Message{
		Type:    protocol.TypeRead,
		Length:  uint32(len(readKey)),
		Payload: keyReader,
	}

	if err := protocol.Encode(conn, readRequestMsg); err != nil {
		t.Error(err)
	}

	time.Sleep(3 * time.Second)
}
