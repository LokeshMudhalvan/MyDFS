package wal

import (
	"encoding/binary"
	"fmt"

	"google.golang.org/protobuf/proto"
)

func (w *WAL) AppendEntry(data []byte) error {
	w.mu.Lock()
	err := w.appendEntry(data)
	flushDone := w.flushDone
	w.mu.Unlock()

	if err != nil {
		return err
	}

	<-flushDone
	fmt.Println("Entry appened to wal log file")
	return nil
}

func (w *WAL) appendEntry(data []byte) error {
	// Check segment size. If segment size has reached max size, create a new segment.
	segSize := w.curSegmentSize

	if segSize >= w.maxSegmentSize {
		if err := w.addNewLogFile(); err != nil {
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
	}

	entryMarshalled, err := w.marshallEntry(entry)
	if err != nil {
		return err
	}

	entryLen := len(entryMarshalled)

	//  If segment size will exceed maxSegmentSize, create a new segment.
	// Add 4 to the total size to take into account the size in bytes of entryLen(uint32)
	if segSize+uint64(entryLen)+4 >= w.maxSegmentSize {
		if err = w.addNewLogFile(); err != nil {
			return err
		}
	}

	if err = binary.Write(w.writer, binary.BigEndian, uint32(entryLen)); err != nil {
		return fmt.Errorf("failed to write size to buffer: %w", err)
	}

	if _, err = w.writer.Write(entryMarshalled); err != nil {
		return fmt.Errorf("failed to write data to buffer: %w", err)
	}

	w.curSegmentSize += uint64(entryLen) + 4
	return nil
}

func (w *WAL) flushBuffer() {
	for {
		select {
		case <-w.flushTimer.C:
			w.mu.Lock()
			if err := w.flush(); err != nil {
				fmt.Println(err)
			}
			w.mu.Unlock()
		case <-w.ctx.Done():
			return
		}
	}
}

func (w *WAL) flush() error {
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush data to buffer: %w", err)
	}

	if w.enableFsSync {
		if err := w.segment.Sync(); err != nil {
			return fmt.Errorf("failed to perform sync: %w", err)
		}
	}

	close(w.flushDone)
	w.flushDone = make(chan struct{})

	return nil
}

func (w *WAL) marshallEntry(entry *WAL_Entry) ([]byte, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, fmt.Errorf("failed to marshall entry: %w", err)
	}

	return data, nil
}
