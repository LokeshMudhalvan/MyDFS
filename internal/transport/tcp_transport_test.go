package transport

import (
	"bytes"
	"net"
	"testing"
)

func TestListen(t *testing.T) {
	tt := []struct {
		test    string
		payload []byte
		want    []byte
	}{
		{
			"sending a simple request",
			[]byte("Hello world\n"),
			[]byte("Recieved: Hello world\n"),
		},
		{
			"sending another simple request",
			[]byte("Hello hello testing\n"),
			[]byte("Recieved: Hello hello testing\n"),
		},
	}

	ts := NewTCPTransport(":5001")
	err := ts.Listen()
	if err != nil {
		t.Error("Failed to establish TCP connection", err)
	}

	for _, tc := range tt {
		t.Run(tc.test, func(t *testing.T) {
			conn, err := net.Dial("tcp", ":5001")
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
				if bytes.Compare(readBuffer[:n], tc.want) == 0 {
					t.Errorf("Want: %s\n Recieved: %s\n", tc.want, string(readBuffer))
				}
			}
		})
	}
}
