package adaptors

import "io"

type WriterAtAdaptper struct {
	w   io.WriterAt
	off int64
}

func NewWriterAtAdapter(w io.WriterAt, off int64) *WriterAtAdaptper {
	return &WriterAtAdaptper{
		w:   w,
		off: off,
	}
}

func (a *WriterAtAdaptper) Write(p []byte) (n int, err error) {
	n, err = a.w.WriteAt(p, a.off)
	a.off += int64(n)
	return n, err
}
