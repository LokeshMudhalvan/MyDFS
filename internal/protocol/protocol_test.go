package protocol

import (
	"io"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: Make the tests more generic, rather than file specific. This is just a placeholder.
func TestEncodeAndDecode(t *testing.T) {
	testFile, err := os.Open("../../test/test1/test-1.txt")
	if err != nil {
		t.Error("failed to open test file", err)
	}

	fileStats, err := testFile.Stat()
	if err != nil {
		t.Error("failed to get file stats", err)
	}
	fileSize := fileStats.Size()

	writeRequestMsg := &Message{
		Type:    TypeWrite,
		Length:  uint32(fileSize),
		Payload: testFile,
	}

	pr, pw := io.Pipe()

	go func() {
		defer pw.Close()
		if err := Encode(pw, writeRequestMsg); err != nil {
			t.Error(err)
		}
	}()

	msg, err := Decode(pr)
	if err != nil {
		t.Error("failed to decode msg", err)
	}

	assertFile, err := os.Open("../../test/test1/test-1.txt")
	if err != nil {
		t.Error("failed to open test file", err)
	}

	data1, _ := io.ReadAll(assertFile)
	data2, _ := io.ReadAll(msg.Payload)
	assert.Equal(t, writeRequestMsg.Type, msg.Type)
	assert.Equal(t, writeRequestMsg.Length, msg.Length)
	assert.Equal(t, string(data1), string(data2))
}
