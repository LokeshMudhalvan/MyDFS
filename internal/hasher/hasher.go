package hasher

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"hash"
	"io"
)

type MD5ContentHasher struct{}

func NewMD5ContentHasher() MD5ContentHasher {
	return MD5ContentHasher{}
}

func (m MD5ContentHasher) HashContent(r io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, r); err != nil {
		return "", fmt.Errorf("failed hashing file content: %w", err)
	}
	decoded := hex.EncodeToString(hash.Sum(nil))
	return decoded, nil
}

func (m MD5ContentHasher) GetHasher() hash.Hash {
	return md5.New()
}

func (m MD5ContentHasher) EncodeToString(hash hash.Hash) string {
	return hex.EncodeToString(hash.Sum(nil))
}

type Hasher func(io.Reader) (string, error)

func MD5ContentHash(r io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, r); err != nil {
		return "", fmt.Errorf("failed hashing file content: %w", err)
	}
	decoded := hex.EncodeToString(hash.Sum(nil))
	return decoded, nil
}
