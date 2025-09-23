package sstable

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/AmrMurad1/Go-Store/shared"
)

type SSManager struct {
	mu       sync.RWMutex
	sstables [][]*SSTable
	dir      string
	config   *SSTableConfig
}

func createPath(dataPath string) error {
	if err := os.MkdirAll(dataPath, 0755); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}
	return nil
}

func (m *SSManager) writeManifestFile() error {
	manifestPath := filepath.Join(m.dir, "manifest")
	file, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create manifest file: %w", err)
	}
	defer file.Close()

	err = binary.Write(file, binary.LittleEndian, int64(len(m.sstables)))
	if err != nil {
		return fmt.Errorf("failed to write manifest file: %w", err)
	}

	for _, level := range m.sstables {
		err = binary.Write(file, binary.LittleEndian, int64(len(level)))
		if err != nil {
			return fmt.Errorf("failed to write manifest file: %w", err)
		}
	}

	return nil
}

func (m *SSManager) recover() ([][]*SSTable, error) {
	manifestPath := filepath.Join(m.dir, "manifest")

	if _, err := os.Stat(manifestPath); os.IsNotExist(err) {
		return m.recoverFromFiles()
	}

	file, err := os.Open(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open manifest: %w", err)
	}
	defer file.Close()

	var numLevels int64
	err = binary.Read(file, binary.LittleEndian, &numLevels)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest: %w", err)
	}

	levels := make([][]*SSTable, numLevels)

	for levelIdx := 0; levelIdx < int(numLevels); levelIdx++ {
		var numSSTables int64
		err = binary.Read(file, binary.LittleEndian, &numSSTables)
		if err != nil {
			return nil, fmt.Errorf("failed to read manifest: %w", err)
		}

		level := make([]*SSTable, 0, numSSTables)

		for i := 0; i < int(numSSTables); i++ {
			filename := fmt.Sprintf("%d.%d.sst", levelIdx, i)
			fullPath := filepath.Join(m.dir, filename)

			if _, err := os.Stat(fullPath); os.IsNotExist(err) {
				log.Printf("Warning: SSTable file %s not found, skipping", filename)
				continue
			}

			sstable, err := Open(fullPath)
			if err != nil {
				log.Printf("Warning: failed to open SSTable %s: %v", filename, err)
				continue
			}

			level = append(level, sstable)
		}

		levels[levelIdx] = level
	}

	return levels, nil
}

func (m *SSManager) recoverFromFiles() ([][]*SSTable, error) {
	files, err := os.ReadDir(m.dir)
	if err != nil {
		return [][]*SSTable{{}}, nil
	}

	levelFiles := make(map[int][]string)

	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sst") {
			continue
		}

		var level, sequence int
		n, err := fmt.Sscanf(file.Name(), "%d.%d.sst", &level, &sequence)
		if n != 2 || err != nil {
			log.Printf("Warning: ignoring file with invalid format: %s", file.Name())
			continue
		}

		levelFiles[level] = append(levelFiles[level], file.Name())
	}

	maxLevel := 0
	for level := range levelFiles {
		if level > maxLevel {
			maxLevel = level
		}
	}

	levels := make([][]*SSTable, maxLevel+1)

	for level := 0; level <= maxLevel; level++ {
		files := levelFiles[level]
		sort.Strings(files)

		levelSSTables := make([]*SSTable, 0, len(files))
		for _, filename := range files {
			fullPath := filepath.Join(m.dir, filename)
			sstable, err := Open(fullPath)
			if err != nil {
				log.Printf("Warning: failed to open SSTable %s: %v", filename, err)
				continue
			}
			levelSSTables = append(levelSSTables, sstable)
		}

		levels[level] = levelSSTables
	}

	return levels, nil
}

func NewSSManager(dir string) (*SSManager, error) {
	config := &SSTableConfig{
		DataBlockSize:           4096,
		FilterFalsePositiveRate: 0.01,
		ExpectedEntryCount:      1000,
	}

	manager := &SSManager{
		dir:    dir,
		config: config,
	}

	err := createPath(dir)
	if err != nil {
		return nil, err
	}

	manager.sstables, err = manager.recover()
	if err != nil {
		return nil, err
	}

	manager.listSSTables()
	return manager, nil
}

func (m *SSManager) listSSTables() {
	fmt.Println("SSTable layout:")
	fmt.Printf("Total levels: %d\n", len(m.sstables))
	for i, level := range m.sstables {
		fmt.Printf("Level %d: %d SSTables\n", i, len(level))
	}
}

func (m *SSManager) Get(key shared.Key) (*shared.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for levelIdx, level := range m.sstables {
		for i := len(level) - 1; i >= 0; i-- {
			sstable := level[i]
			entry, err := sstable.Get(key)
			if err != nil {
				log.Printf("Error searching SSTable in level %d, index %d: %v", levelIdx, i, err)
				continue
			}

			if entry != nil {
				if entry.Tombstone {
					return nil, nil
				}
				log.Printf("Key found in level %d, SSTable %d", levelIdx, i)
				return entry, nil
			}
		}
	}

	return nil, nil
}

func (m *SSManager) AddSSTable(sstable *SSTable) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.sstables) == 0 {
		m.sstables = append(m.sstables, []*SSTable{})
	}

	m.sstables[0] = append(m.sstables[0], sstable)

	err := m.fixLevels()
	if err != nil {
		return fmt.Errorf("compaction failed: %w", err)
	}

	m.listSSTables()
	return nil
}

func (m *SSManager) fixLevels() error {
	for levelIdx := 0; levelIdx < len(m.sstables); levelIdx++ {
		level := m.sstables[levelIdx]

		if len(level) >= 2 {
			log.Printf("Starting compaction: level %d -> level %d", levelIdx, levelIdx+1)

			if len(m.sstables) == levelIdx+1 {
				m.sstables = append(m.sstables, []*SSTable{})
			}

			nextLevel := levelIdx + 1
			newFilename := fmt.Sprintf("%s/%d.%d.sst", m.dir, nextLevel, len(m.sstables[nextLevel]))

			compactedSSTable, err := m.compactSSTables(level, newFilename, nextLevel == len(m.sstables)-1)
			if err != nil {
				return fmt.Errorf("failed to compact level %d: %w", levelIdx, err)
			}

			if compactedSSTable != nil {
				m.sstables[nextLevel] = append(m.sstables[nextLevel], compactedSSTable)
			}

			for _, oldSSTable := range level {
				oldSSTable.Close()
			}
			m.sstables[levelIdx] = []*SSTable{}

			log.Printf("Level %d compacted successfully", levelIdx)
		}
	}

	return nil
}

func (m *SSManager) compactSSTables(sstables []*SSTable, outputPath string, deleteTombstones bool) (*SSTable, error) {
	if len(sstables) == 0 {
		return nil, nil
	}

	if len(sstables) == 1 {
		return sstables[0], nil
	}

	merged := sstables[0]
	var err error

	for i := 1; i < len(sstables); i++ {
		newOutput := fmt.Sprintf("%s.tmp.%d", outputPath, i)

		merged, err = compact(newOutput, merged, sstables[i], deleteTombstones, m.config)
		if err != nil {
			return nil, err
		}
	}

	if err := os.Rename(fmt.Sprintf("%s.tmp.%d", outputPath, len(sstables)-1), outputPath); err != nil {
		return nil, fmt.Errorf("failed to rename compacted SSTable: %w", err)
	}

	return Open(outputPath)
}

func (m *SSManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstError error

	for _, level := range m.sstables {
		for _, sstable := range level {
			if err := sstable.Close(); err != nil && firstError == nil {
				firstError = err
			}
		}
	}

	if err := m.writeManifestFile(); err != nil && firstError == nil {
		firstError = err
	}

	m.sstables = nil

	return firstError
}
