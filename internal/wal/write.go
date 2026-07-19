package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"
)

func (w *WAL) AppendEntry(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.appendEntry(data, false)
}

func (w *WAL) appendEntry(data []byte, isCheckpoint bool) error {
	// Check segment size. If segment size has reached max size, create a new segment.
	segSize, err := w.getSegmentSize()
	if err != nil {
		return err
	}

	if segSize >= int64(w.maxSegmentSize) {
		if err = w.addNewLogFile(); err != nil {
			return err
		}
	}

	w.lastSequenceNo++
	seqNo := w.lastSequenceNo
	crc := w.generateCRC(data)

	entry := &WAL_Entry{
		LogSequenceNo: seqNo,
		Data:          data,
		CRC:           crc,
		IsCheckpoint:  &isCheckpoint,
	}

	entryMarshalled, err := w.marshallEntry(entry)
	if err != nil {
		return err
	}

	entryLen := len(entryMarshalled)

	//  If segment size will exceed maxSegmentSize, create a new segment.
	if segSize+int64(entryLen) >= int64(w.maxSegmentSize) {
		if err = w.addNewLogFile(); err != nil {
			return err
		}
	}

	var b bytes.Buffer

	if err = binary.Write(&b, binary.BigEndian, uint32(entryLen)); err != nil {
		return fmt.Errorf("failed to write size to buffer: %w", err)
	}

	if _, err = b.Write(entryMarshalled); err != nil {
		return fmt.Errorf("failed to write data to buffer: %w", err)
	}

	if _, err = io.Copy(w.segment, &b); err != nil {
		return fmt.Errorf("failed to write data to segment: %w", err)
	}

	if w.enableFsSync {
		if err = w.segment.Sync(); err != nil {
			return fmt.Errorf("failed to perform sync: %w", err)
		}
	}
	// TODO: Implement checkpoint logic
	if isCheckpoint {
		fmt.Println("Checkpoint not implemented yet. Checkpoint to be implemented")
	}
	return nil
}

func (w *WAL) marshallEntry(entry *WAL_Entry) ([]byte, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshall entry: %w", err)
	}

	return data, nil
}
