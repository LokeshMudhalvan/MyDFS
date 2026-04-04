package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MagicByte1 = 0xAC
	MagicByte2 = 0xFA
	Headersize = 8
	Version    = 1
	MaxPayload = 64 * (1 << 20) // 64MB chunks
)

type MessageType uint8

// Message Types
const (
	TypeWrite MessageType = iota
	TypeRead
)

var (
	ErrInvalidMagic       = errors.New("invalid magic bytes")
	ErrUnsupportedVersion = errors.New("unsupported protocol version")
	ErrPayloadTooLarge    = errors.New("payload exceeds maximum allowed size")
)

type Message struct {
	version     uint8
	messageType MessageType
	payload     []byte
}

func Encode(w io.Writer, m *Message) error {
	header := make([]byte, Headersize)

	header[0] = MagicByte1
	header[1] = MagicByte2

	header[2] = m.version
	header[3] = byte(m.messageType)

	binary.BigEndian.AppendUint32(header[4:8], uint32(len(m.payload)))

	if _, err := w.Write(header); err != nil {
		return fmt.Errorf("failed to encode data and write header: %w", err)
	}

	if len(m.payload) > 0 {
		if _, err := w.Write(m.payload); err != nil {
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

	if payloadLen > MaxPayload {
		return nil, ErrPayloadTooLarge
	}

	payload := make([]byte, payloadLen)

	if payloadLen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil, fmt.Errorf("failed to decode payload: %w", err)
		}
	}

	return &Message{
		version:     version,
		messageType: MessageType(messageType),
		payload:     payload,
	}, nil
}
