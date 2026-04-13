package hasher

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
)

type Hasher func(io.Reader) (string, error)

func MD5ContentHash(r io.Reader) (string, error) {
	hash := md5.New()
	if _, err := io.Copy(hash, r); err != nil {
		return "", fmt.Errorf("failed hashing file content: %w", err)
	}
	decoded := hex.EncodeToString(hash.Sum(nil))
	return decoded, nil
}
