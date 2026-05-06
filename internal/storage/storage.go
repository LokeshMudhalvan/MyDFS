package storage

import (
	"fmt"
	"hash"
	"io"
	"os"
	"strings"
)

const (
	FalseLength = -1 // return FalseLength for read errors as length
)

type FileMetaData struct {
	FullPath    fullPath
	ContentHash string
}

type Storage interface {
	Write(string, io.Reader) (FileMetaData, error)
	Read(string) (io.Reader, int64, error)
	RenameFile(string) error
}

type Hasher interface {
	GetHasher() hash.Hash
	EncodeToString(hash.Hash) string
}

type pathTransformFunc func(string, int) (fullPath, error)

type FileStorage struct {
	pathTransformFunc pathTransformFunc
	depth             int // depth determines how nested the folder should be
	hasher            Hasher
}

func NewFileStorage(pathTransformFunc pathTransformFunc, depth int, hasher Hasher) *FileStorage {
	return &FileStorage{
		pathTransformFunc: pathTransformFunc,
		depth:             depth,
		hasher:            hasher,
	}
}

func (f *FileStorage) Write(key string, data io.Reader) (FileMetaData, error) {
	path, err := f.getFilePath(key)
	if err != nil {
		return FileMetaData{}, fmt.Errorf("failed getting file path: %w", err)
	}
	// TEST: Just for testing purpouses. Remove this later
	path.basePath = "./test-write/" + path.basePath

	if err := os.MkdirAll(path.basePath, os.ModePerm); err != nil {
		return FileMetaData{}, fmt.Errorf("failed creating directories: %w", err)
	}

	filePath := path.GetFilePath()
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return FileMetaData{}, fmt.Errorf("failed opening file to write: %w", err)
	}
	defer file.Close()
	hasher := f.hasher.GetHasher()
	w := io.MultiWriter(file, hasher)
	// TODO:Create a multi writer to write to both the file and the hasher to compute content hash.
	if _, err := io.Copy(w, data); err != nil {
		return FileMetaData{}, fmt.Errorf("failed writing data to file: %w", err)
	}

	contentHash := f.hasher.EncodeToString(hasher)

	return FileMetaData{
		FullPath:    path,
		ContentHash: contentHash,
	}, nil
}

func (f *FileStorage) Read(key string) (io.Reader, int64, error) {
	path, err := f.getFilePath(key)
	if err != nil {
		return nil, FalseLength, fmt.Errorf("failed getting file path for key: %w", err)
	}

	filePath := path.basePath + "/" + path.fileName
	file, err := os.Open(filePath)
	if err != nil {
		return nil, FalseLength, fmt.Errorf("failed opening file : %w", err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		return nil, FalseLength, fmt.Errorf("failed getting file status: %w", err)
	}
	fileLen := fileStat.Size()
	fileContents := io.LimitReader(file, fileLen)

	return fileContents, fileLen, nil
}

func (f *FileStorage) RenameFile(oldPath string) error {
	newPath := oldPath[:strings.LastIndex(oldPath, ".")]
	return os.Rename(oldPath, newPath)
}

func (f *FileStorage) getFilePath(key string) (fullPath, error) {
	path, err := f.pathTransformFunc(key, f.depth)
	if err != nil {
		return fullPath{}, err
	}

	return path, nil
}
