package storage

import (
	"fmt"
	"io"
	"os"
)

const (
	FalseLength = -1 // return FalseLength for read errors as length
)

type Storage interface {
	Write(string, io.Reader) error
	Read(string) (io.Reader, int64, error)
}

type pathTransformFunc func(string, int) (fullPath, error)

type FileStorage struct {
	pathTransformFunc pathTransformFunc
	depth             int // depth determines how nested the folder should be
}

func NewFileStorage(pathTransformFunc pathTransformFunc, depth int) *FileStorage {
	return &FileStorage{
		pathTransformFunc: pathTransformFunc,
		depth:             depth,
	}
}

func (f *FileStorage) getFilePath(key string) (fullPath, error) {
	path, err := f.pathTransformFunc(key, f.depth)
	if err != nil {
		return fullPath{}, err
	}

	return path, nil
}

func (f *FileStorage) Write(key string, data io.Reader) error {
	path, err := f.getFilePath(key)
	if err != nil {
		return fmt.Errorf("failed getting file path: %w", err)
	}

	if err := os.MkdirAll(path.basePath, os.ModePerm); err != nil {
		return fmt.Errorf("failed creating directories: %w", err)
	}

	filePath := path.basePath + "/" + path.fileName
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed opening file to write: %w", err)
	}
	defer file.Close()
	if _, err := io.Copy(file, data); err != nil {
		return fmt.Errorf("failed writing data to file: %w", err)
	}

	return nil
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
