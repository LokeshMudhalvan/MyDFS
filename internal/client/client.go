package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"sync"
	"time"

	"github.com/lokeshMudhalvan/MyDFS/internal/encoder"
	"github.com/lokeshMudhalvan/MyDFS/internal/files"
	"github.com/lokeshMudhalvan/MyDFS/internal/hasher"
	"github.com/lokeshMudhalvan/MyDFS/internal/metaserver"
	"github.com/lokeshMudhalvan/MyDFS/internal/protocol"
	"github.com/lokeshMudhalvan/MyDFS/internal/transport"
	workers "github.com/lokeshMudhalvan/MyDFS/internal/wokers"
	"google.golang.org/protobuf/proto"
)

const (
	ChunkSize = 64 * (1 << 20) // 64MB chunks
	// ChunkSize              = 2 // TEST: This is only a test value
	MaxMetadataSizeInBytes = 4 // Max Metadata length is 2^32 - 1 ~ 4GB
)

var (
	ErrWriteCancelled = errors.New("write operation cancelled. Try again")
	ErrReadCancelled  = errors.New("read operation cancelled. Try again")
)

type Hasher interface {
	HashContent(io.Reader) (string, error)
}

type Client struct {
	protocol protocol.Protocol
	hasher   Hasher
	// Encoder to serialize resulting structs
	encoder encoder.Encoder
	config  *clientConfig
	// Connection pool to connect to the chunk servers
	connPool        transport.TransportPool
	readWorkerPool  workers.WorkerPool
	writeWorkerPool workers.WorkerPool
	metaServer      *metaserver.MetaServer
	ctx             context.Context
	wg              sync.WaitGroup
}

type readConfig struct {
	workers         int
	readTimeout     time.Duration
	maxRetries      int
	retryDelay      time.Duration
	shutdownTimeout time.Duration
}

type writeConfig struct {
	workers         int
	writeTimeout    time.Duration
	maxRetries      int
	retryDelay      time.Duration
	shutdownTimeout time.Duration
}

type transportConfig struct {
	connections        uint16
	dialTimeout        time.Duration
	healthCheckTimeout time.Duration
	shutdownTimeout    time.Duration
}

type clientConfig struct {
	shutdownTimeout time.Duration
	readConfig      *readConfig
	writeConfig     *writeConfig
	transportConfig *transportConfig
}

type ClientOption func(*Client)

func WithProtcol(protocol protocol.Protocol) ClientOption {
	return func(c *Client) {
		c.protocol = protocol
	}
}

func WithHasher(hasher Hasher) ClientOption {
	return func(c *Client) {
		c.hasher = hasher
	}
}

func WithEncoder(encoder encoder.Encoder) ClientOption {
	return func(c *Client) {
		c.encoder = encoder
	}
}

func WithReadWorkerCount(workers int) ClientOption {
	return func(c *Client) {
		c.config.readConfig.workers = workers
	}
}

func WithReadTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.readConfig.readTimeout = timeout
	}
}

func WithReadMaxRetries(retries int) ClientOption {
	return func(c *Client) {
		c.config.readConfig.maxRetries = retries
	}
}

func WithReadRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.config.readConfig.retryDelay = delay
	}
}

func WithReadShutdownTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.readConfig.shutdownTimeout = timeout
	}
}

func WithWriteWorkerCount(workers int) ClientOption {
	return func(c *Client) {
		c.config.writeConfig.workers = workers
	}
}

func WithWriteTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.writeConfig.writeTimeout = timeout
	}
}

func WithWriteMaxRetries(retries int) ClientOption {
	return func(c *Client) {
		c.config.writeConfig.maxRetries = retries
	}
}

func WithWriteRetryDelay(delay time.Duration) ClientOption {
	return func(c *Client) {
		c.config.writeConfig.retryDelay = delay
	}
}

func WithWriteShutdownTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.writeConfig.shutdownTimeout = timeout
	}
}

func WithTransportPoolConnectionCount(connections uint16) ClientOption {
	return func(c *Client) {
		c.config.transportConfig.connections = connections
	}
}

func WithTransportHealthCheckTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.transportConfig.healthCheckTimeout = timeout
	}
}

func WithTransportDialTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.transportConfig.dialTimeout = timeout
	}
}

func WithTransportShutdownTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.transportConfig.shutdownTimeout = timeout
	}
}

func WithTimeoutShutdown(timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.config.shutdownTimeout = timeout
	}
}

func defaultClient() *Client {
	readConf := &readConfig{
		workers:         6,
		readTimeout:     5 * time.Second,
		maxRetries:      3,
		retryDelay:      2 * time.Second,
		shutdownTimeout: 5 * time.Second,
	}

	writeConf := &writeConfig{
		workers:         6,
		writeTimeout:    8 * time.Second,
		maxRetries:      3,
		retryDelay:      2 * time.Second,
		shutdownTimeout: 5 * time.Second,
	}

	tansportConf := &transportConfig{
		connections:        5,
		dialTimeout:        3 * time.Second,
		healthCheckTimeout: 1 * time.Second,
		shutdownTimeout:    5 * time.Second,
	}

	conf := &clientConfig{
		shutdownTimeout: 10 * time.Second,
		readConfig:      readConf,
		writeConfig:     writeConf,
		transportConfig: tansportConf,
	}
	return &Client{
		protocol: protocol.NewMessageTransferProtocol(),
		hasher:   hasher.NewMD5ContentHasher(),
		encoder:  encoder.NewGobEncoder(),
		config:   conf,
	}
}

func NewClient(ctx context.Context, opts ...ClientOption) (*Client, error) {
	c := defaultClient()
	c.ctx = ctx

	for _, opt := range opts {
		opt(c)
	}

	// TCP pool to connect to the metadata server
	connPool, err := transport.NewTCPPool(
		c.ctx,
		// TODO: Change this to use metaServer address without hard coding
		":5002",
		transport.WithDialTimeout(c.config.transportConfig.dialTimeout),
		transport.WithMaxConn(c.config.transportConfig.connections),
		transport.WithProtocol(c.protocol),
		transport.WithPoolShutdownTimeout(c.config.transportConfig.shutdownTimeout),
	)
	if err != nil {
		return nil, err
	}
	c.connPool = connPool
	c.readWorkerPool = workers.NewRetriableWorkerPool(
		c.ctx,
		workers.WithMinWorkers(c.config.readConfig.workers/2),
		workers.WithMaxWorkers(c.config.readConfig.workers),
		workers.WithBufferSize(c.config.readConfig.workers),
		workers.WithRetryDelay(c.config.readConfig.retryDelay),
		workers.WithMaxRetries(c.config.readConfig.maxRetries),
		workers.WithShutdownTimeout(c.config.readConfig.shutdownTimeout),
	)
	c.writeWorkerPool = workers.NewRetriableWorkerPool(
		c.ctx,
		workers.WithMinWorkers(c.config.writeConfig.workers/2),
		workers.WithMaxWorkers(c.config.writeConfig.workers),
		workers.WithBufferSize(c.config.writeConfig.workers),
		workers.WithRetryDelay(c.config.writeConfig.retryDelay),
		workers.WithMaxRetries(c.config.writeConfig.maxRetries),
		workers.WithShutdownTimeout(c.config.writeConfig.shutdownTimeout),
	)

	return c, nil
}

func (c *Client) SendFile(ctx context.Context, filePath string) error {
	file, err := os.Open(filePath)
	defer file.Close()
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	fileStat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stats: %w", err)
	}

	fileSize := fileStat.Size()
	chunks, err := c.chunkFiles(file, fileSize)
	if err != nil {
		return err
	}

	fMeta := c.buildFileMetadata(chunks, fileStat)

	updatedMeta, err := c.registerFileMetadata(ctx, fMeta)
	if err != nil {
		return err
	}

	c.updateChunkInfo(updatedMeta, chunks)

	results, err := c.sendChunks(chunks)
	if err != nil {
		return err
	}

	for result := range results {
		if result.Status == workers.StatusCancelled {
			return ErrWriteCancelled
		}
		if result.Status == workers.StatusFailed {
			return fmt.Errorf("write failed: %w", result.Error)
		}
		chunkMeta, ok := result.Output.(files.ChunkMetaData)
		if !ok {
			return fmt.Errorf("failed to type cast result output to chunk meta data")
		}
		fmt.Println("successfully wrote chunk: ", chunkMeta.Id)
	}

	return nil
}

func (c *Client) ReadFile(ctx context.Context, name string, filePath string) error {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.ModePerm)
	defer file.Close()
	if err != nil {
		return err
	}

	meta, err := c.fetchFileMetadata(ctx, name)
	if err != nil {
		return err
	}
	results, err := c.processReadFile(meta, file)
	if err != nil {
		return err
	}
	for result := range results {
		if result.Status == workers.StatusCancelled {
			os.Remove(filePath)
			return ErrReadCancelled
		}
		if result.Status == workers.StatusFailed {
			os.Remove(filePath)
			return fmt.Errorf("read failed: %w", result.Error)
		}
	}

	return nil
}

func (c *Client) Close() {
	c.wg.Add(1)
	go func() {
		c.readWorkerPool.Shutdown()
		c.wg.Done()
	}()

	c.wg.Add(1)
	go func() {
		c.writeWorkerPool.Shutdown()
		c.wg.Done()
	}()

	c.wg.Add(1)
	go func() {
		c.connPool.ClosePool()
		c.wg.Done()
	}()

	done := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(c.config.shutdownTimeout):
		fmt.Println("Client closed after timeout")
	}
}

func (c *Client) requestMetaServer(ctx context.Context, msg *protocol.Message) ([]byte, error) {
	dialTimeoutCtx, cancel := context.WithTimeout(ctx, c.config.transportConfig.dialTimeout)
	defer cancel()
	conn, err := c.connPool.Get(dialTimeoutCtx)
	if err != nil {
		return nil, err
	}

	if err = c.protocol.Encode(conn, msg); err != nil {
		return nil, err
	}

	resp, err := c.protocol.Decode(conn)
	if err != nil {
		return nil, err
	}
	payloadBytes, err := io.ReadAll(resp.Payload)
	if err != nil {
		return nil, fmt.Errorf("failed to read payload: %w", err)
	}
	return payloadBytes, nil
}

func (c *Client) buildFileMetadata(chunks []*files.Chunk, stat fs.FileInfo) *files.FileMetadata {
	chunkInfo := make(map[string]*files.ChunkInfo)

	for _, chunk := range chunks {
		chunkInfo[chunk.Metadata.Id] = chunk.Metadata.ChunkInfo
	}
	fMeta := &files.FileMetadata{
		Size:      stat.Size(),
		Name:      stat.Name(),
		ChunkInfo: chunkInfo,
	}

	return fMeta
}

func (c *Client) fetchFileMetadata(ctx context.Context, file string) (*files.FileMetadata, error) {
	r := bytes.NewReader([]byte(file))
	msg := protocol.NewMessage(protocol.TypeReadFile, r, uint32(r.Len()))
	payloadBytes, err := c.requestMetaServer(ctx, msg)
	if err != nil {
		return nil, err
	}

	var meta files.FileMetadata
	if err = proto.Unmarshal(payloadBytes, &meta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return &meta, nil
}

func (c *Client) registerFileMetadata(ctx context.Context, meta *files.FileMetadata) (*files.FileMetadata, error) {
	fMetaBytes, err := proto.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal fMeta: %w", err)
	}
	fMetaReader := bytes.NewReader(fMetaBytes)
	msg := protocol.NewMessage(protocol.TypeAddFile, fMetaReader, uint32(fMetaReader.Len()))
	payloadBytes, err := c.requestMetaServer(ctx, msg)
	if err != nil {
		return nil, err
	}

	var updatedMeta files.FileMetadata
	if err = proto.Unmarshal(payloadBytes, &updatedMeta); err != nil {
		return nil, fmt.Errorf("failed to unmarshal recieved file metadata: %w", err)
	}

	return &updatedMeta, nil
}

func (c *Client) updateChunkInfo(meta *files.FileMetadata, chunks []*files.Chunk) {
	for _, chunk := range chunks {
		chunk.Metadata.ChunkInfo = meta.ChunkInfo[chunk.Metadata.Id]
	}
}

func (c *Client) getChunkServerConn(ctx context.Context, addr string) (net.Conn, error) {
	d := &net.Dialer{}

	dialTimeoutCtx, cancel := context.WithTimeout(ctx, c.config.transportConfig.dialTimeout)
	defer cancel()

	conn, err := d.DialContext(dialTimeoutCtx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to chunkserver: %w", err)
	}

	if err = conn.SetWriteDeadline(time.Now().Add(c.config.writeConfig.writeTimeout)); err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to set write deadline for connection: %w", err)
	}

	return conn, nil
}
