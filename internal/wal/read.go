package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"
)

// TODO: reset entries if a entry is already in a snapshot
func (w *WAL) ReadAllEntries() ([]*WAL_Entry, error) {
	var entries []*WAL_Entry
	c := w.countLogFiles()
	if c == 0 {
		return entries, ErrNoLogFiles
	}

	w.mu.Lock()
	segNo := w.lastSegmentNo
	// Read only till the sequence number at current point in time. Consistent Snapshot Isolation.
	seqNo := w.lastSequenceNo
	w.mu.Unlock()

	startNo := segNo - c + 1

	i := 0
	// Read all entries from each wal file
	for uint64(i) < c {
		filePath := w.generateLogFilePath(startNo)
		f, err := os.OpenFile(filePath, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return entries, fmt.Errorf("failed to open wal log file: %w", err)
		}
		partialEntries, err := w.readAllEntriesFromFile(f, seqNo)
		if err != nil {
			return entries, fmt.Errorf("failed to read enteries from wal log file: %w", err)
		}
		entries = append(entries, partialEntries...)

		i += 1
		startNo += 1
	}

	return entries, nil
}

func (w *WAL) readAllEntriesFromFile(f *os.File, seqNo uint64) ([]*WAL_Entry, error) {
	var entries []*WAL_Entry
	defer f.Close()
	for {
		entry, err := w.readEntry(f)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return entries, nil
			}

			return entries, err
		}

		entries = append(entries, entry)

		if entry.LogSequenceNo == seqNo {
			return entries, nil
		}
	}
}

func (w *WAL) readEntry(f *os.File) (*WAL_Entry, error) {
	var entry *WAL_Entry
	var length uint32

	if err := binary.Read(f, binary.BigEndian, &length); err != nil {
		return entry, fmt.Errorf("failed to decode wal entry length: %w", err)
	}

	entryData := make([]byte, length)
	if _, err := io.ReadFull(f, entryData); err != nil {
		return entry, fmt.Errorf("failed to read wal")
	}

	entry, err := w.unmarshallEntry(entryData)
	if err != nil {
		return entry, err
	}

	return entry, nil
}

func (w *WAL) unmarshallEntry(data []byte) (*WAL_Entry, error) {
	entry := &WAL_Entry{}
	if err := proto.Unmarshal(data, entry); err != nil {
		return nil, fmt.Errorf("failed to unmarshall data: %w", err)
	}

	if !w.verifyCRC(entry) {
		return nil, ErrCRCVerificationFailed
	}

	return entry, nil
}
