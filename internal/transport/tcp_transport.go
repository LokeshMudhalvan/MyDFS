package transport

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/storage"
)

var ErrTCPAccpet = errors.New("TCP Accept Error")

type Transport interface {
	Listen()
	Close()
}

type TCPTransport struct {
	wg           sync.WaitGroup
	listenerPort string
	listener     net.Listener
	shutdown     chan struct{}
	connections  chan net.Conn
	storage      storage.Storage // INFO: This is only for testing purpouses. REMOVE this later
}

func NewTCPTransport(listenerPort string) *TCPTransport {
	storage := storage.NewFileStorage(storage.SHA256PathTransform, 5)
	return &TCPTransport{
		listenerPort: listenerPort,
		shutdown:     make(chan struct{}),
		connections:  make(chan net.Conn),
		storage:      storage, // INFO: This is only for testing purpouses. REMOVE this later
	}
}

func (t *TCPTransport) Listen() error {
	listener, err := net.Listen("tcp", t.listenerPort)
	if err != nil {
		return err
	}
	t.listener = listener
	t.wg.Add(2)
	go t.acceptConnections()
	go t.handleConnections()
	return nil
}

func (t *TCPTransport) Close() {
	close(t.shutdown)
	t.listener.Close()

	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return
	case <-time.After(5 * time.Second):
		fmt.Println("Timed out waiting for connections to finish.")
		return
	}
}

func (t *TCPTransport) acceptConnections() error {
	defer t.wg.Done()

	for {
		select {
		case <-t.shutdown:
			return nil
		default:
			conn, err := t.listener.Accept()
			if err != nil {
				return ErrTCPAccpet
			}
			t.connections <- conn
		}
	}
}

func (t *TCPTransport) handleConnections() {
	defer t.wg.Done()

	for {
		select {
		case <-t.shutdown:
			return
		case conn := <-t.connections:
			t.handleConnection(conn)
		}
	}
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	fmt.Println("Recieved Connection")
	defer conn.Close()

	// TODO: This is a placeholder func. To be replaced by an actual handler to read or write data.
	readBuffer := make([]byte, 64)
	for {
		n, err := conn.Read(readBuffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println("An error occured:", err)
			}
			break
		}
		data := string(readBuffer[:n])
		fmt.Printf("Recieved: %s", data)
		_, err = conn.Write(readBuffer[:n])
		if err != nil {
			fmt.Println("An error occured:", err)
		}

		err = t.storage.Write("mothersbestimages", readBuffer[:n])
		if err != nil {
			fmt.Printf("Error writing to storage: %w", err)
		}

	}
}
