package wal

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

func (w *WAL) ReadAllEntries() error {
	return nil
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
