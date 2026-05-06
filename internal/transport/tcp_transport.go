package transport

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/handler"
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
	handler      handler.Handler
}

func NewTCPTransport(listenerPort string, handler handler.Handler) *TCPTransport {
	return &TCPTransport{
		listenerPort: listenerPort,
		shutdown:     make(chan struct{}),
		connections:  make(chan net.Conn),
		handler:      handler,
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
			select {
			case t.connections <- conn:
			// connection sent
			case <-t.shutdown:
				conn.Close()
				return nil
			}
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
			t.wg.Add(1)

			go func(conn net.Conn) {
				defer t.wg.Done()
				t.handleConnection(conn)
			}(conn)
		}
	}
}

func (t *TCPTransport) handleConnection(conn net.Conn) {
	defer conn.Close()

	if err := t.handler.Handle(conn); err != nil {
		fmt.Println("An error occured handling recieved message:", err)
	}
}
