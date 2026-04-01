package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var ErrorNegativeFileDepth = errors.New("File Depth cannot be negative")

type fullPath struct {
	fileName string
	basePath string
}

func SHA256PathTransform(key string, depth int) (fullPath, error) {
	hasher := sha256.New()
	hasher.Write([]byte(key))
	decoded := hex.EncodeToString(hasher.Sum(nil))
	basePath := ""

	if depth == 0 {
		return fullPath{
			basePath: ".",
			fileName: decoded,
		}, nil
	}

	if depth < 0 {
		return fullPath{}, ErrorNegativeFileDepth
	}

	split := len(decoded) / depth
	start := 0

	for i := 0; i < depth; i++ {
		if i == depth-1 {
			basePath += decoded[start:]
		} else {
			basePath += decoded[start:start+split] + "/"
			start += split
		}
	}

	return fullPath{
		basePath: basePath,
		fileName: decoded,
	}, nil
}
