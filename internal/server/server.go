package server

import "github.com/lokeshMudhalvan/MyDFS/internal/files"

type MetaServer struct {
	fileStore *files.FileStore
}

func NewMetaServer(walDir string) (*MetaServer, error) {
	fileStore, err := files.NewFileStore(walDir)
	if err != nil {
		return nil, err
	}
	return &MetaServer{
		fileStore: fileStore,
	}, nil
}

// TODO: Needs to specify which chunk servers to write to for each chunk
func (m *MetaServer) HandleWrite(meta *files.FileMetadata) error {
	return m.fileStore.AddFileMetadata(meta)
}

// Takes file name as input
func (m *MetaServer) HandleRead(name string) *files.FileMetadata {
	return m.fileStore.LookupFileMetadata(name)
}

func (m *MetaServer) HandleDelete(name string) error {
	return m.fileStore.DeleteFileMetadata(name)
}
