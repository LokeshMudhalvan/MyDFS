package transport

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
)

var (
	ErrTCPAccept            = errors.New("TCP Accept error")
	ErrTypeCastingToTCPConn = errors.New("error type casting conn to TCP conn")
	ErrPoolClosed           = errors.New("TCP connection pool has been closed")
)

type TransportPool interface {
	Get(context.Context) (net.Conn, error)
	Put(net.Conn)
	ClosePool()
}

type TCPPool struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	addr        string
	maxConn     uint16
	timeout     time.Duration
	connections chan net.Conn
	isClosed    bool
	protocol    protocol.Protocol
}

func NewTCPPool(ctx context.Context, protocol protocol.Protocol, addr string, maxConn uint16, timeout time.Duration) (*TCPPool, error) {
	t := &TCPPool{
		addr:        addr,
		maxConn:     maxConn,
		timeout:     timeout,
		connections: make(chan net.Conn, maxConn),
		protocol:    protocol,
	}

	errCh := make(chan error)
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for i := 0; i < int(maxConn); i++ {
		t.wg.Add(1)

		go func() {
			defer t.wg.Done()

			if t.connections == nil {
				return
			}
			conn, err := t.createNewConnection(timeoutCtx)
			if err != nil {
				errCh <- fmt.Errorf("failed to create new tcp connection: %w", err)
				return
			}

			t.mu.Lock()
			defer t.mu.Unlock()
			if t.isClosed {
				fmt.Println("Pool is closed, not adding connection to pool")
				conn.Close()
				return
			}

			t.connections <- conn
		}()
	}

	go func() {
		t.wg.Wait()
		defer close(errCh)
	}()

	for err := range errCh {
		cancel()
		t.ClosePool()
		return nil, err
	}

	return t, nil
}

func (t *TCPPool) Get(ctx context.Context) (net.Conn, error) {
	t.mu.Lock()

	if t.isClosed {
		fmt.Println("Pool is closed already, skipping Put operation")
		return nil, ErrPoolClosed
	}

	t.mu.Unlock()

	select {
	case conn := <-t.connections:
		if t.performHealthCheck(conn) {
			return conn, nil
		}
		fmt.Println("failed health check")
		conn.Close()
		return t.createNewConnection(ctx)
	default:
		return t.createNewConnection(ctx)
	}
}

func (t *TCPPool) Put(conn net.Conn) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		fmt.Println("Pool is closed already, skipping Put operation")
		return
	}

	select {
	case t.connections <- conn:
	default:
		conn.Close()
	}
}

func (t *TCPPool) ClosePool() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		fmt.Println("Pool is closed already skippping ClosePool")
		return
	}

	t.isClosed = true

	close(t.connections)
	for conn := range t.connections {
		conn.Close()
	}
}

func (t *TCPPool) createNewConnection(ctx context.Context) (net.Conn, error) {
	d := &net.Dialer{}

	conn, err := d.DialContext(ctx, "tcp", t.addr)
	if err != nil {
		return nil, ErrTCPAccept
	}

	return conn, nil
}

func (t *TCPPool) performHealthCheck(conn net.Conn) bool {
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return false
	}
	msg := protocol.NewMessage(protocol.TypePing, bytes.NewBuffer([]byte("PING")), uint32(len("PING")))

	if err := t.protocol.Encode(conn, msg); err != nil {
		return false
	}

	resp, err := t.protocol.Decode(conn)
	if err != nil {
		return false
	}

	readBuffer, err := io.ReadAll(resp.Payload)
	if err != nil {
		return false
	}

	if string(readBuffer) == "PING" {
		return true
	}

	return false
}
