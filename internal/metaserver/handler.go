package metaserver

import (
	"bytes"
	"fmt"
	"io"
	"net"

	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"google.golang.org/protobuf/proto"
)

type MetaStore interface {
	AddFileMetadata(*files.FileMetadata, bool) error
	LookupFileMetadata(string) *files.FileMetadata
	DeleteFileMetadata(string) error
}

type MetaServerHandler struct {
	metastore MetaStore
	protocol  protocol.Protocol
}

func NewMetaServerHandler(metastore MetaStore, protocol protocol.Protocol) *MetaServerHandler {
	return &MetaServerHandler{
		metastore: metastore,
		protocol:  protocol,
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

	meta := m.metastore.LookupFileMetadata(string(name))
	b, err := proto.Marshal(meta)
	if err != nil {
		return fmt.Errorf("failed to marshal file metadata: %w", err)
	}

	payload := bytes.NewReader(b)
	msg := protocol.NewMessage(protocol.TypeReadFileResponse, payload, uint32(payload.Len()))
	if err = m.encode(conn, msg); err != nil {
		return err
	}

	return nil
}

func (m *MetaServerHandler) handleWrite(r io.Reader, conn net.Conn) error {
	byte, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("failed to read write request: %w", err)
	}
	var meta files.FileMetadata
	if err := proto.Unmarshal(byte, &meta); err != nil {
		return err
	}

	for k := range meta.ChunkInfo {
		meta.ChunkInfo[k].Addr = ":5001"
	}

	if err = m.metastore.AddFileMetadata(&meta, true); err != nil {
		return err
	}

	metaBytes, err := proto.Marshal(&meta)
	if err != nil {
		return fmt.Errorf("failed to marshall file metadata: %w", err)
	}

	payload := bytes.NewReader(metaBytes)
	msg := protocol.NewMessage(protocol.TypeReadFileResponse, payload, uint32(payload.Len()))
	if err = m.encode(conn, msg); err != nil {
		return err
	}

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
