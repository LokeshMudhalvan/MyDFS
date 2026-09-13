package metaserver

import (
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
)

type MetaStore interface {
	AddFileMetadata(*files.FileMetadata, bool) error
	LookupFileMetadata(string) *files.FileMetadata
	DeleteFileMetadata(string) error
}

type MetaServerHandler struct {
	metastore MetaStore
	protocol  protocol.Protocol
	encoder   encoder.Encoder
}

func NewMetaServerHandler(metastore MetaStore, protocol protocol.Protocol, encoder encoder.Encoder) *MetaServerHandler {
	return &MetaServerHandler{
		metastore: metastore,
		protocol:  protocol,
		encoder:   encoder,
	}
}

func (m *MetaServerHandler) Handle(conn net.Conn) error {
	for {
		msg, err := m.decode(conn)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		if msg.Length == 0 {
			fmt.Printf("Recieved connection message with length 0. Skipping it.")
			return nil
		}

		switch msg.Type {
		case protocol.TypePing:
			if err := m.handlePing(msg.Payload, conn); err != nil {
				return err
			}
		case protocol.TypeAddFile:
			if err := m.handleWrite(msg.Payload, conn); err != nil {
				return err
			}
		case protocol.TypeReadFile:
			if err := m.handleRead(msg.Payload, conn); err != nil {
				return err
			}
		case protocol.TypeDeleteFile:
			if err := m.handleDelete(msg.Payload); err != nil {
				return err
			}
		default:
			fmt.Println("Unkown message type. Skipping.")
		}
	}
}

func (m *MetaServerHandler) handlePing(r io.Reader, conn net.Conn) error {
	msg, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("error reading ping message: %w", err)
	}
	response := protocol.NewMessage(protocol.TypePingResponse, bytes.NewBuffer(msg), uint32(len(msg)))

	if err := m.protocol.Encode(conn, response); err != nil {
		return err
	}

	return nil
}

func (m *MetaServerHandler) handleRead(r io.Reader, conn net.Conn) error {
	name, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read file name to read: %w", err)
	}

	var metaBuffer bytes.Buffer
	meta := m.metastore.LookupFileMetadata(string(name))
	if err = m.encoder.Encode(&metaBuffer, meta); err != nil {
		return err
	}

	msg := protocol.NewMessage(protocol.TypeReadFileResponse, &metaBuffer, uint32(metaBuffer.Len()))
	if err = m.protocol.Encode(conn, msg); err != nil {
		return err
	}

	return nil
}

// TODO: Needs to specify which chunk servers to write to for each chunk
func (m *MetaServerHandler) handleWrite(r io.Reader, conn net.Conn) error {
	var meta files.FileMetadata
	if err := m.encoder.Decode(r, &meta); err != nil {
		return err
	}
	m.metastore.AddFileMetadata(&meta, true)
	return nil
}

func (m *MetaServerHandler) handleDelete(r io.Reader) error {
	name, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read file name to read: %w", err)
	}

	m.metastore.DeleteFileMetadata(string(name))
	return nil
}

func (m *MetaServerHandler) decode(conn net.Conn) (*protocol.Message, error) {
	msg, err := m.protocol.Decode(conn)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func (m *MetaServerHandler) encode(conn net.Conn, msg *protocol.Message) error {
	if err := m.protocol.Encode(conn, msg); err != nil {
		return err
	}

	return nil
}
