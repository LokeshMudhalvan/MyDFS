package wal

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TODO: Change implementation of snapshot too.
const (
	WalLogPrefix = "wal-log-"
	SnapshotFile = "wal-snapshot"
)

var (
	ErrMaxSegmentSizeZero = errors.New("max segment size cannot be zero")
	ErrMaxSegments        = errors.New("max segments cannot be zero")
	// TODO: Handle this error to go back to previous log file and read
	ErrReadEmptyLogFile      = errors.New("log file is empty, cannot be read")
	ErrCRCVerificationFailed = errors.New("failed CRC verification")
	ErrNoLogFiles            = errors.New("no log files exist in the WAL dir")
)

// TODO: Ensure the file close is handled properly, the same method should cancel the snapshot goroutine's context
// TODO: Handle Deletion: Add isDelete property to WAL_Entry protobuf
type WAL struct {
	dir          string
	enableFsSync bool
	// maxSegmentSize refers to the maximum allowed size in bytes of a wal log file
	maxSegmentSize uint64
	curSegmentSize uint64
	// flushTimer refers to how often batched writes are written to the log
	flushTimer *time.Ticker
	flushDone  chan struct{}
	writer     *bufio.Writer
	segment    *os.File
	// maxSegements refers to the maximum number of wal log files allowed in the wal dir at any given point of time
	maxSegements   uint64
	lastSegmentNo  uint64
	lastSequenceNo uint64
	// snapshotInterval refers to the elapsed time between two snapshots
	snapshotTimer *time.Ticker
	// lastSnapshot maintains the last log sequence number of the last snapshot
	lastSnapshot uint64
	snapshotable Snapshotable
	mu           sync.Mutex
	ctx          context.Context
	cancel       context.CancelFunc
}

type WALOption func(*WAL)

func EnableFsSync() WALOption {
	return func(w *WAL) {
		w.enableFsSync = true
	}
}

func WithMaxSegementSize(maxSegmentSize uint64) WALOption {
	return func(w *WAL) {
		w.maxSegmentSize = maxSegmentSize
	}
}

func WithMaxSegements(maxSegments uint64) WALOption {
	return func(w *WAL) {
		w.maxSegements = maxSegments
	}
}

func WithFlushInterval(flushInterval time.Duration) WALOption {
	return func(w *WAL) {
		w.flushTimer = time.NewTicker(flushInterval)
	}
}

func WithSnapshotInterval(snapshotInterval time.Duration) WALOption {
	return func(w *WAL) {
		w.snapshotTimer = time.NewTicker(snapshotInterval)
	}
}

// TEST: Just a test method to manually close segment file. Create a separate method to handle WALClose
func (w *WAL) TestCloseSegment() {
	w.segment.Close()
}

func defaultWALConfig() *WAL {
	ctx, cancel := context.WithCancel(context.Background())
	return &WAL{
		enableFsSync:   false,
		maxSegmentSize: 64,
		maxSegements:   3,
		snapshotTimer:  time.NewTicker(2 * time.Minute),
		flushTimer:     time.NewTicker(5 * time.Millisecond),
		flushDone:      make(chan struct{}),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// TODO: Create method to take snapshots
func InitWAL(dir string, snapshotable Snapshotable, opts ...WALOption) (*WAL, error) {
	w := defaultWALConfig()
	w.dir = dir
	w.snapshotable = snapshotable

	for _, opt := range opts {
		opt(w)
	}

	if !w.checkPreExistingWALFiles() {
		fmt.Println("There are no pre-existing WAL files. Initializing WAL")
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("failed to create WAL dir: %w", err)
		}
		if err := w.addNewLogFile(); err != nil {
			return nil, fmt.Errorf("failed to add new log file: %w", err)
		}
	} else {
		// Update last segment number and last sequence number if WAL for initialized already
		// TODO: Find lastSnapshot and update it
		segmentNo, err := w.findLastSegmentNumber()
		if err != nil {
			return nil, err
		}
		fmt.Println("This is the last segment number: ", segmentNo)

		w.lastSegmentNo = segmentNo
		filePath := w.generateLogFilePath(segmentNo)
		f, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, os.ModePerm)
		if err != nil {
			return nil, fmt.Errorf("failed to open wal log file: %w", err)
		}
		stat, err := f.Stat()
		if err != nil {
			return nil, fmt.Errorf("failed to get wal log stats: %w", err)
		}
		w.curSegmentSize = uint64(stat.Size())
		w.segment = f
		w.writer = bufio.NewWriter(w.segment)
		seqNo, err := w.findLastSequenceNumberInLogFile(w.segment)
		fmt.Println("This is the last sequence number: ", seqNo)
		if err != nil {
			return nil, err
		}
		w.lastSequenceNo = seqNo
	}

	go w.flushBuffer()
	go w.snapshotRunner()
	return w, nil
}

func (w *WAL) checkPreExistingWALFiles() bool {
	fileCount := w.countLogFiles()

	return fileCount > 0
}

func (w *WAL) countLogFiles() uint64 {
	fileCount := 0
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return 0
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), WalLogPrefix) {
			fileCount += 1
		}
	}

	fmt.Println("This is the log file count: ", fileCount)
	return uint64(fileCount)
}

func (w *WAL) findLastSequenceNumberInLogFile(f *os.File) (uint64, error) {
	entry, err := w.findLastRecord(f)
	if err != nil {
		return 0, err
	}

	return entry.GetLogSequenceNo(), nil
}

func (w *WAL) findLastRecord(f *os.File) (*WAL_Entry, error) {
	var prevLength uint32
	var entry *WAL_Entry

	for {
		var length uint32

		if err := binary.Read(f, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				offset, err := f.Seek(0, io.SeekCurrent)
				if err != nil {
					return nil, fmt.Errorf("failed to seek file offset: %w", err)
				}
				if offset == 0 {
					return nil, ErrReadEmptyLogFile
				}

				if _, err := f.Seek(-int64(prevLength), io.SeekCurrent); err != nil {
					return nil, fmt.Errorf("failed to seek file offset: %w", err)
				}

				entryData := make([]byte, prevLength)
				if _, err := io.ReadFull(f, entryData); err != nil {
					return nil, fmt.Errorf("failed to read log entry data: %w", err)
				}

				entry, err = w.unmarshallEntry(entryData)
				if err != nil {
					return nil, err
				}

				return entry, nil
			}

			return nil, fmt.Errorf("failed to read size of log entry: %w", err)
		}

		if _, err := f.Seek(int64(length), io.SeekCurrent); err != nil {
			return nil, fmt.Errorf("failed to seek in segment file: %w", err)
		}
		prevLength = length

	}
}

func (w *WAL) findLastSegmentNumber() (uint64, error) {
	lastSegmentNo := 0
	files, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, fmt.Errorf("failed to read wal dir: %w", err)
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasPrefix(file.Name(), WalLogPrefix) {
			fileSplit := strings.Split(file.Name(), "-")
			curFileSegmentNo, err := strconv.Atoi(fileSplit[len(fileSplit)-1])
			if err != nil {
				return 0, fmt.Errorf("failed to convert file segment number to integer: %w", err)
			}
			lastSegmentNo = max(lastSegmentNo, curFileSegmentNo)
		}
	}
	return uint64(lastSegmentNo), nil
}

func (w *WAL) addNewLogFile() error {
	fileCount := w.countLogFiles()
	if fileCount >= w.maxSegements {
		err := w.removeLogFile()
		if err != nil {
			return err
		}
	}

	w.lastSegmentNo++
	segNo := w.lastSegmentNo

	filePath := w.generateLogFilePath(segNo)
	f, err := os.Create(filePath)
	// Store the current log file as segement
	if err != nil {
		return fmt.Errorf("failed to create new log file: %w", err)
	}
	w.segment = f
	w.curSegmentSize = 0
	// Flush contents within buffer before switching to new file buffer
	if w.writer != nil {
		if err := w.flush(); err != nil {
			return fmt.Errorf("failed to flush buffer contents to log file: %w", err)
		}
	}
	w.writer = bufio.NewWriter(w.segment)
	return nil
}

func (w *WAL) removeLogFile() error {
	// Compute segment number to remove
	segNo := w.lastSegmentNo - w.maxSegements + 1
	fullPath := w.generateLogFilePath(segNo)
	f, err := os.Open(fullPath)
	if err != nil {
		return fmt.Errorf("failed to open log file: %w", err)
	}

	seqNo, err := w.findLastSequenceNumberInLogFile(f)
	if err != nil {
		return err
	}

	// Do not delete log file if last LSN is greater than the last snapshot's last LSN
	// These files will be cleaned up after the next snapshot
	if seqNo > w.lastSnapshot {
		return nil
	}

	if err = os.Remove(fullPath); err != nil {
		return fmt.Errorf("failed to remove wal file: %w", err)
	}

	return nil
}

func (w *WAL) snapshotRunner() {
	select {
	case <-w.snapshotTimer.C:
		err := w.takeSnapshot()
		if err != nil {
			fmt.Println("Error occurred while taking snapshot: ", err)
		}
	case <-w.ctx.Done():
		return
	}
}
