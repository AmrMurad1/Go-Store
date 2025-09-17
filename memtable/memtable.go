package memtable

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/AmrMurad1/Go-Store/shared"
)

type Memtable struct {
	mu       *sync.RWMutex
	skiplist *SkipList
	wal      *Wal
	size     int
}

func NewMemtable(walDir string) (*Memtable, error) {
	wal, err := NewWal(walDir, "wal.log")
	if err != nil {
		return nil, err
	}

	m := &Memtable{
		mu:       &sync.RWMutex{},
		skiplist: New(18, 0.5),
		wal:      wal,
		size:     0,
	}

	if err := m.recover(walDir); err != nil {
		return nil, err
	}

	return m, nil
}

func (m *Memtable) recover(walDir string) error {
	files, err := os.ReadDir(walDir)
	if err != nil {
		return fmt.Errorf("could not read WAL directory: %w", err)
	}

	var walFiles []*Wal
	for _, file := range files {
		if !file.IsDir() && filepath.Ext(file.Name()) == ".log" {
			// Open each WAL file found
			oldWal, err := NewWal(walDir, file.Name())
			if err != nil {
				// Decide how to handle this - skip, log, or fail?
				// For now, let's return the error.
				return fmt.Errorf("could not open old WAL file %s: %w", file.Name(), err)
			}
			walFiles = append(walFiles, oldWal)
		}
	}

	// In a more complex system, you would sort these files by a version number.
	// For now, we process them in the order the OS gives them.

	for _, oldWal := range walFiles {
		entries, err := oldWal.Retrieve()
		if err != nil {
			return fmt.Errorf("could not retrieve entries from %s: %w", oldWal.path, err)
		}

		for _, entry := range entries {
			// Set the entry in the skiplist
			sizeChange := m.skiplist.Set(shared.Entry{Key: shared.Key(entry.Key), Value: entry.Value})
			m.size += sizeChange

			// Write the entry to the *new* WAL file to consolidate it
			if err := m.wal.Append(entry); err != nil {
				return fmt.Errorf("could not append to new WAL: %w", err)
			}
		}

		// Delete the old WAL file now that it has been processed
		if err := oldWal.Delete(); err != nil {
			return fmt.Errorf("could not delete old WAL file %s: %w", oldWal.path, err)
		}
	}

	return nil
}

func (m *Memtable) Set(key shared.Key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	entry := shared.Entry{
		Key:   key,
		Value: value,
	}

	walEntry := WALEntry{
		Key:   string(key),
		Value: value,
	}

	if err := m.wal.Append(walEntry); err != nil {
		return err
	}

	sizeChange := m.skiplist.Set(entry)
	m.size += sizeChange
	return nil
}

func (m *Memtable) Get(key shared.Key) (shared.Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.skiplist.Get(key)
}




