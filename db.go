package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/AmrMurad1/Go-Store/memtable"
	"github.com/AmrMurad1/Go-Store/shared"
	"github.com/AmrMurad1/Go-Store/sstable"
)

type Engine struct {
	memtable        *memtable.Memtable
	sstableManager  *sstable.SSManager
	dir             string
	lock            *sync.Mutex
	maxMemtableSize int
}

func NewEngine(dir string) (*Engine, error) {
	db := &Engine{
		dir:             dir,
		lock:            &sync.Mutex{},
		maxMemtableSize: 1024 * 1024, // 1MB default
	}

	log.Printf("setup data path: %s...\n", db.dir)

	var err error
	db.memtable, err = memtable.NewMemtable(dir)
	if err != nil {
		log.Printf("setup failed: %v", err)
		return nil, err
	}

	db.sstableManager, err = sstable.NewSSManager(dir)
	if err != nil {
		log.Printf("setup failed: %v", err)
		return nil, err
	}

	log.Println("setup done")
	return db, nil
}

func (db *Engine) Close() error {
	return db.sstableManager.Close()
}

func (db *Engine) Get(key string) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	sharedKey := shared.Key(key)
	entry, found := db.memtable.Get(sharedKey)
	if found {
		if !entry.Tombstone {
			return string(entry.Value), nil
		} else {
			return "", fmt.Errorf("key does not exist")
		}
	}

	ssEntry, err := db.sstableManager.Get(sharedKey)
	if err != nil {
		return "", err
	}

	if ssEntry != nil && !ssEntry.Tombstone {
		return string(ssEntry.Value), nil
	}

	return "", fmt.Errorf("key does not exist")
}

func (db *Engine) Set(key string, val string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	sharedKey := shared.Key(key)
	err := db.memtable.Set(sharedKey, []byte(val))
	if err != nil {
		return err
	}

	if db.memtable.Size() >= db.maxMemtableSize {
		log.Println("full table")
		log.Println("loading to disk...")
		err := db.flushToDisk()
		if err != nil {
			return err
		}
		db.memtable, err = memtable.NewMemtable(db.dir)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Engine) Delete(key string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	sharedKey := shared.Key(key)
	err := db.memtable.Delete(sharedKey)
	if err != nil {
		return err
	}

	if db.memtable.Size() >= db.maxMemtableSize {
		log.Println("full table")
		log.Println("loading to disk...")
		err = db.flushToDisk()
		if err != nil {
			return err
		}
		db.memtable, err = memtable.NewMemtable(db.dir)
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Engine) flushToDisk() error {
	entries := db.memtable.All()
	if len(entries) == 0 {
		return nil
	}

	config := &sstable.SSTableConfig{
		DataBlockSize:           4096,
		FilterFalsePositiveRate: 0.01,
		ExpectedEntryCount:      1000,
	}

	filename := fmt.Sprintf("%s/temp.sst", db.dir)
	writer, err := sstable.NewBlockWriter(filename, config)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		writer.Add(entry)
	}

	writer.Finish()

	newSSTable, err := sstable.Open(filename)
	if err != nil {
		return err
	}

	return db.sstableManager.AddSSTable(newSSTable)
}
