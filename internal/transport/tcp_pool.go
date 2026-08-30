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
	mu                 sync.Mutex
	wg                 sync.WaitGroup
	addr               string
	maxConn            uint16
	dialTimeout        time.Duration
	healthCheckTimeout time.Duration
	shutdownTimeout    time.Duration
	connections        chan net.Conn
	ctx                context.Context
	isClosed           bool
	protocol           protocol.Protocol
}

type TCPPoolOption func(*TCPPool)

func defaultTCPPoolConfig() *TCPPool {
	t := &TCPPool{
		maxConn:            uint16(5),
		healthCheckTimeout: 2 * time.Second,
		shutdownTimeout:    5 * time.Second,
		dialTimeout:        5 * time.Second,
		protocol:           protocol.NewChunkTransferProtocol(),
	}

	return t
}

func WithMaxConn(maxConn uint16) TCPPoolOption {
	return func(t *TCPPool) {
		t.maxConn = maxConn
	}
}

func WithProtocol(protocol protocol.Protocol) TCPPoolOption {
	return func(t *TCPPool) {
		t.protocol = protocol
	}
}

func WithDialTimeout(timeout time.Duration) TCPPoolOption {
	return func(t *TCPPool) {
		t.dialTimeout = timeout
	}
}

func WithPoolShutdownTimeout(timeout time.Duration) TCPPoolOption {
	return func(t *TCPPool) {
		t.shutdownTimeout = timeout
	}
}

func NewTCPPool(ctx context.Context, addr string, opts ...TCPPoolOption) (*TCPPool, error) {
	t := defaultTCPPoolConfig()
	t.addr = addr
	t.ctx = ctx
	t.connections = make(chan net.Conn, t.maxConn)

	for _, opt := range opts {
		opt(t)
	}

	groupCtx, cancel := context.WithCancel(t.ctx)
	defer cancel()

	var (
		once     sync.Once
		firstErr error
	)

	for i := 0; i < int(t.maxConn); i++ {
		t.wg.Add(1)

		go func() {
			defer t.wg.Done()

			if t.connections == nil {
				return
			}
			timeoutCtx, cancel := context.WithTimeout(groupCtx, t.dialTimeout)
			defer cancel()

			conn, err := t.createNewConnection(timeoutCtx)
			if err != nil {
				once.Do(
					func() {
						firstErr = fmt.Errorf("failed to create new tcp connection: ", err)
						cancel()
					},
				)
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

	t.wg.Wait()

	if firstErr != nil {
		t.ClosePool()
		return nil, firstErr
	}

	return t, nil
}

func (t *TCPPool) Get(ctx context.Context) (net.Conn, error) {
	t.mu.Lock()

	if t.isClosed {
		t.mu.Unlock()
		fmt.Println("Pool is closed already, skipping Put operation")
		return nil, ErrPoolClosed
	}

	t.mu.Unlock()

	select {
	case conn, ok := <-t.connections:
		if !ok {
			return nil, ErrPoolClosed
		}
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
	done := make(chan struct{})

	t.wg.Add(1)
	go func() {
		for conn := range t.connections {
			conn.Close()
		}
		t.wg.Done()
	}()

	go func() {
		t.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(t.shutdownTimeout):
		fmt.Println("TCP pool shutdown after timeout")
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
	if err := conn.SetReadDeadline(time.Now().Add(t.healthCheckTimeout)); err != nil {
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
		if err = conn.SetReadDeadline(time.Time{}); err != nil {
			return false
		}
		return true
	}

	return false
}
