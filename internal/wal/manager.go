package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	WalLogPrefix = "wal-log-"
)

var (
	ErrMaxSegmentSizeZero = errors.New("max segment size cannot be zero")
	ErrMaxSegments        = errors.New("max segments cannot be zero")
	// TODO: Handle this error to go back to previous log file and read
	ErrEmptyLogFile          = errors.New("log file is empty, cannot be read")
	ErrCRCVerificationFailed = errors.New("failed CRC verification")
)

// TODO: Ensure the file close is handled properly
type WAL struct {
	dir          string
	enableFsSync bool
	// maxSegmentSize refers to the maximum allowed size in bytes of a wal log file
	maxSegmentSize uint64
	segment        *os.File
	// maxSegements refers to the maximum number of wal log files allowed in the wal dir at any given point of time
	maxSegements   uint64
	lastSegmentNo  uint64
	lastSequenceNo uint64
	mu             sync.Mutex
}

// TEST: Just a test method to manually close segment file. Create a separate method to handle WALClose
func (w *WAL) TestCloseSegment() {
	w.segment.Close()
}

// TODO: Create method to take snapshots, i.e. handle checkpointing
func InitWAL(dir string, enableFsSync bool, maxSegmentSize uint64, maxSegments uint64) (*WAL, error) {
	w := &WAL{
		dir:            dir,
		enableFsSync:   enableFsSync,
		maxSegmentSize: maxSegmentSize,
		maxSegements:   maxSegments,
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

		w.segment = f
		seqNo, err := w.findLastSequenceNumber()
		fmt.Println("This is the last sequence number: ", seqNo)
		if err != nil {
			return nil, err
		}
		w.lastSequenceNo = seqNo
	}

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

func (w *WAL) findLastSequenceNumber() (uint64, error) {
	entry, err := w.findLastRecord()
	if err != nil {
		return 0, err
	}

	return entry.GetLogSequenceNo(), nil
}

func (w *WAL) findLastRecord() (*WAL_Entry, error) {
	var prevLength uint32
	var entry *WAL_Entry

	file := w.segment
	for {
		var length uint32

		if err := binary.Read(file, binary.BigEndian, &length); err != nil {
			if err == io.EOF {
				offset, err := file.Seek(0, io.SeekCurrent)
				if err != nil {
					return nil, fmt.Errorf("failed to seek file offset: %w", err)
				}
				if offset == 0 {
					return nil, ErrEmptyLogFile
				}

				if _, err := file.Seek(-int64(prevLength), io.SeekCurrent); err != nil {
					return nil, fmt.Errorf("failed to seek file offset: %w", err)
				}

				entryData := make([]byte, prevLength)
				if _, err := io.ReadFull(file, entryData); err != nil {
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

		if _, err := file.Seek(int64(length), io.SeekCurrent); err != nil {
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
	w.segment = f
	if err != nil {
		return fmt.Errorf("failed to create new log file: %w", err)
	}
	return nil
}

func (w *WAL) removeLogFile() error {
	// Compute segment number to remove
	segNo := w.lastSegmentNo - w.maxSegements + 1

	fullPath := w.generateLogFilePath(segNo)
	err := os.Remove(fullPath)
	if err != nil {
		return fmt.Errorf("failed to remove wal file: %w", err)
	}

	return nil
}

func (w *WAL) getSegmentSize() (int64, error) {
	segNo := w.lastSegmentNo

	filePath := w.generateLogFilePath(segNo)
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open wal log file: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("failed to get wal log file stats: %w", err)
	}

	return stat.Size(), nil
}
