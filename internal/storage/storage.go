package storage

import (
	"fmt"
	"os"
)

type Storage interface {
	Write(string, []byte) error
	Read()
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

func (f *FileStorage) Write(key string, data []byte) error {
	path, err := f.getFilePath(key)
	if err != nil {
		return fmt.Errorf("Error getting file path: %w", err)
	}

	err = os.MkdirAll(path.basePath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Error creating directories: %w", err)
	}

	filePath := path.basePath + "/" + path.fileName
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("Error opening file to write: %w", err)
	}
	defer file.Close()
	_, err = file.Write(data)
	if err != nil {
		return fmt.Errorf("Error writing data to file: %w", err)
	}

	return nil
}

func (f *FileStorage) Read() {
}
