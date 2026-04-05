package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MagicByte1    = 0xAC
	MagicByte2    = 0xFA
	Headersize    = 8
	Version       = 1
	MaxPayloadLen = 64 * (1 << 20) // 64MB chunks
)

type MessageType uint8

// Message Types
const (
	// Requests
	TypeWrite MessageType = iota
	TypeRead

	// Responses
	TypeReadResponse
	TypeWriteResponse
)

var (
	ErrInvalidMagic       = errors.New("invalid magic bytes")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
	ErrPayloadTooLarge    = errors.New("payload exceeds maximum allowed size")
)

type Message struct {
	Type    MessageType
	Length  uint32
	Payload io.Reader
}

func Encode(w io.Writer, m *Message) error {
	if m.Length > MaxPayloadLen {
		return ErrPayloadTooLarge
	}

	header := make([]byte, Headersize)

	header[0] = MagicByte1
	header[1] = MagicByte2

	header[2] = Version
	header[3] = byte(m.Type)

	binary.BigEndian.PutUint32(header[4:8], m.Length)
	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("failed to encode data and write header: %w", err)
	}

	if m.Length > 0 {
		if m.Payload == nil {
			return fmt.Errorf("payload is nil")
		}
		if _, err := io.CopyN(w, m.Payload, int64(m.Length)); err != nil {
			return fmt.Errorf("failed to encode data and write payload: %w", err)
		}
	}
	return nil
}

func Decode(r io.Reader) (*Message, error) {
	header := make([]byte, Headersize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, fmt.Errorf("failed to decode header: %w", err)
	}

	if header[0] != MagicByte1 || header[1] != MagicByte2 {
		return nil, ErrInvalidMagic
	}

	version := header[2]
	if version != Version {
		return nil, ErrUnsupportedVersion
	}

	messageType := header[3]
	payloadLen := binary.BigEndian.Uint32(header[4:8])

	if payloadLen > MaxPayloadLen {
		return nil, ErrPayloadTooLarge
	}

	payload := io.LimitReader(r, int64(payloadLen))

	return &Message{
		Type:    MessageType(messageType),
		Payload: payload,
		Length:  payloadLen,
	}, nil
}
