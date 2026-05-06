package storage

import (
	"errors"
)

var ErrNegativeFileDepth = errors.New("file depth cannot be negative")

type fullPath struct {
	fileName string
	basePath string
}

// serves as a method to combine fileName and basePath to give full file path
func (f fullPath) GetFilePath() string {
	return f.basePath + "/" + f.fileName
}

func HashPathTransform(key string, depth int) (fullPath, error) {
	basePath := ""

	if depth == 0 {
		return fullPath{
			basePath: ".",
			fileName: key,
		}, nil
	}

	if depth < 0 {
		return fullPath{}, ErrNegativeFileDepth
	}

	split := len(key) / depth
	start := 0

	for i := 0; i < depth; i++ {
		if i == depth-1 {
			basePath += key[start:]
		} else {
			basePath += key[start:start+split] + "/"
			start += split
		}
	}

	return fullPath{
		basePath: basePath,
		fileName: key + ".tmp",
	}, nil
}
