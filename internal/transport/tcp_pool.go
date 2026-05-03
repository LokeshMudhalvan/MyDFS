package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	ErrTCPAccept            = errors.New("TCP Accept error")
	ErrTypeCastingToTCPConn = errors.New("error type casting conn to TCP conn")
	ErrPoolClosed           = errors.New("TCP connection pool has been closed")
)

type TransportPool interface {
	Get()
	Put()
	ClosePool()
}

type TCPPool struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	addr        string
	minConn     uint16
	maxConn     uint16
	totalConn   uint16
	timeout     time.Duration
	connections chan *net.TCPConn
	isClosed    bool
}

func NewTCPPool(ctx context.Context, addr string, minConn uint16, maxConn uint16, timeout time.Duration) (*TCPPool, error) {
	t := &TCPPool{
		addr:        addr,
		maxConn:     maxConn,
		minConn:     minConn,
		timeout:     timeout,
		connections: make(chan *net.TCPConn, maxConn),
	}

	errCh := make(chan error)
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for i := 0; i < int(minConn); i++ {
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

			t.totalConn++
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

func (t *TCPPool) Get(ctx context.Context) (*net.TCPConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		fmt.Println("Pool is closed already, skipping Put operation")
		return nil, ErrPoolClosed
	}

	select {
	case conn := <-t.connections:
		if t.performHealthCheck(conn) {
			return conn, nil
		}
		return t.createNewConnection(ctx)
	default:
		return t.createNewConnection(ctx)
	}
}

func (t *TCPPool) Put(conn *net.TCPConn) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.isClosed {
		fmt.Println("Pool is closed already, skipping Put operation")
		return
	}

	select {
	case t.connections <- conn:
		fmt.Println("TCP connection put back to tcp connections")
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
		t.totalConn -= 1
		conn.Close()
	}
}

func (t *TCPPool) createNewConnection(ctx context.Context) (*net.TCPConn, error) {
	d := &net.Dialer{}

	conn, err := d.DialContext(ctx, "tcp", t.addr)
	if err != nil {
		return nil, ErrTCPAccept
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		return nil, ErrTypeCastingToTCPConn
	}
	return tcpConn, nil
}

func (t *TCPPool) performHealthCheck(conn *net.TCPConn) bool {
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return false
	}

	if _, err := conn.Write([]byte("PING")); err != nil {
		return false
	}

	readBuffer := make([]byte, 4)
	if _, err := conn.Read(readBuffer); err != nil {
		return false
	}

	if string(readBuffer) == "PING" {
		return true
	}

	return false
}
