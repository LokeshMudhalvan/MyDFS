package transport

import (
	"context"
	"testing"
	"time"
)

func TestTCPPool(t *testing.T) {
	ctx := context.Background()
	h := &MockHandler{}
	ts := NewTCPTransport(":5001", h)
	err := ts.Listen()
	defer ts.Close()
	if err != nil {
		t.Error("Failed to establish TCP connection", err)
	}

	p, err := NewTCPPool(
		ctx,
		":5001",
		5,
		10,
		5*time.Second,
	)
	if err != nil {
		t.Fatalf("failed to instantiate tcp pool: %s", err)
	}

	t.Logf("total connections: %d", p.totalConn)
	timelineCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := p.Get(timelineCtx)
	if err != nil {
		t.Fatalf("failed to get tcp conn from tcp pool: %s", err)
	}

	test := []byte("Hello")
	if _, err := conn.Write(test); err != nil {
		t.Fatalf("failed to write to tcp conn: %s", err)
	}
	t.Logf("Waiting for read \n")
	reader := make([]byte, 5)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(reader); err != nil {
		t.Fatalf("failed to read from tcp conn: %s", err)
	}

	t.Logf("recieved: %s", string(reader))

	if string(reader) != "Hello" {
		t.Fatalf("read string is: %s, expected string is: Hello", string(reader))
	}

	p.Put(conn)

	t.Logf("total connections: %d", p.totalConn)

	p.ClosePool()
	t.Logf("total connections: %d", p.totalConn)
}
