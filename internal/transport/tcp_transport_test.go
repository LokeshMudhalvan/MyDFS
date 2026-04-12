package transport

import (
	"bytes"
	"io"
	"net"
	"testing"
)

type MockHandler struct{}

func (m *MockHandler) Handle(conn net.Conn) error {
	reader := make([]byte, 64)
	for {
		n, err := conn.Read(reader)
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		conn.Write(reader[:n])
	}

	return nil
}

func TestListen(t *testing.T) {
	tt := []struct {
		test    string
		payload []byte
		want    []byte
	}{
		{
			"sending a simple request",
			[]byte("Hello world\n"),
			[]byte("Hello world\n"),
		},
		{
			"sending another simple request",
			[]byte("Hello hello testing\n"),
			[]byte("Hello hello testing\n"),
		},
	}

	h := &MockHandler{}
	ts := NewTCPTransport(":0", h)
	err := ts.Listen()
	defer ts.Close()
	if err != nil {
		t.Error("Failed to establish TCP connection", err)
	}

	for _, tc := range tt {
		t.Run(tc.test, func(t *testing.T) {
			addr := ts.listener.Addr().String()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				t.Error("Failed to connect to TCP server", err)
			}

			defer conn.Close()
			if _, err := conn.Write(tc.payload); err != nil {
				t.Error("Failed to write payload to TCP server", err)
			}

			readBuffer := make([]byte, 64)
			if n, err := conn.Read(readBuffer); err != nil {
				t.Error("Failed to read payload", err)
			} else {
				if bytes.Compare(readBuffer[:n], tc.want) != 0 {
					t.Errorf("Want: %s\n Recieved: %s\n", tc.want, string(readBuffer))
				}
			}
		})
	}
}
