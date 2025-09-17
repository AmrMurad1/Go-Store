package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const KeySize = 256 // Fixed key size

type WALEntry struct {
	Key   string
	Value []byte
}

type Wal struct {
	mu     sync.Mutex
	writer io.WriteCloser
	dir    string
	path   string
}

func NewWal(dir, filename string) (*Wal, error) {
	w := &Wal{
		dir:  dir,
		path: filepath.Join(dir, filename),
	}
	return w, w.Open()
}

func (w *Wal) Open() error {
	file, err := os.OpenFile(w.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("WAL %q cannot open file: %v", w.path, err)
	}

	w.writer = file
	return nil
}

func (w *Wal) Append(entry WALEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	buf := make([]byte, 0, KeySize+4+len(entry.Value))
	buf = append(buf, []byte(entry.Key)...)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(entry.Value))) // Value length
	buf = append(buf, entry.Value...)

	_, err := w.writer.Write(buf)
	return err
}

func (w *Wal) Retrieve() ([]WALEntry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	file, err := os.Open(w.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := bytes.NewBuffer(nil)
	_, err = io.Copy(buf, file)
	if err != nil {
		return nil, err
	}

	mp := map[string][]byte{}

	for buf.Len() > 0 {
		keyBytes := make([]byte, KeySize)
		buf.Read(keyBytes)

		lenBytes := make([]byte, 4)
		buf.Read(lenBytes)
		valueLen := binary.LittleEndian.Uint32(lenBytes)

		value := make([]byte, valueLen)
		buf.Read(value)

		mp[string(keyBytes)] = value
	}

	var entries []WALEntry
	for k, v := range mp {
		entries = append(entries, WALEntry{Key: k, Value: v})
	}
	return entries, nil
}

func (w *Wal) Clear() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return os.Truncate(w.path, 0)
}

func (w *Wal) Close() error {

	return w.writer.Close()
}
