package encoder

import (
	"encoding/gob"
	"fmt"
	"io"
)

// Encoder Interface allows to encode custom struct to binary
// for streaming data through TCP
type Encoder interface {
	Encode(io.Writer, any) error
	Decode(io.Reader, any) error
}

type GobEncoder struct{}

func NewGobEncoder() *GobEncoder {
	return &GobEncoder{}
}

func (g *GobEncoder) Encode(w io.Writer, v any) error {
	enc := gob.NewEncoder(w)

	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("failed to encode struct: %w", err)
	}

	return nil
}

func (g *GobEncoder) Decode(r io.Reader, v any) error {
	dec := gob.NewDecoder(r)

	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("failed to decode to struct: %w", err)
	}

	return nil
}
