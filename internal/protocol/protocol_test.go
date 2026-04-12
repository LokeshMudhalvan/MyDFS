package protocol

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncodeAndDecode(t *testing.T) {
	protocol := NewChunkTransferProtocol()
	testContent := []byte("Hello World")
	testReader := bytes.NewReader(testContent)

	writeRequestMsg := &Message{
		Type:    TypeWrite,
		Length:  uint32(len(testContent)),
		Payload: testReader,
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if err := protocol.Encode(pw, writeRequestMsg); err != nil {
			t.Error(err)
		}
	}()

	msg, err := protocol.Decode(pr)
	if err != nil {
		t.Error("failed to decode msg", err)
	}

	data1 := string(testContent)
	data2, _ := io.ReadAll(msg.Payload)
	assert.Equal(t, writeRequestMsg.Type, msg.Type)
	assert.Equal(t, writeRequestMsg.Length, msg.Length)
	assert.Equal(t, data1, string(data2))
	assert.Nil(t, err)
}
