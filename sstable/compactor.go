package sstable

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/AmrMurad1/Go-Store/shared"
	"github.com/klauspost/compress/s2"
)

type SSTableIterator struct {
	sstable      *SSTable
	blockIdx     int
	entryIdx     int
	currentBlock []shared.Entry
	finished     bool
}

func (st *SSTable) newIterator() (*SSTableIterator, error) {
	return &SSTableIterator{
		sstable: st,
	}, nil
}

func (it *SSTableIterator) seekStart() error {
	it.blockIdx = 0
	it.entryIdx = 0
	it.finished = false
	return it.loadCurrentBlock()
}

func (it *SSTableIterator) loadCurrentBlock() error {
	if it.blockIdx >= len(it.sstable.indexRecords) {
		it.finished = true
		return nil
	}

	record := it.sstable.indexRecords[it.blockIdx]
	dataBlockBytes := make([]byte, record.Size)
	_, err := it.sstable.file.ReadAt(dataBlockBytes, record.Offset)
	if err != nil {
		return err
	}

	decompressedBlock, err := s2.Decode(nil, dataBlockBytes)
	if err != nil {
		return err
	}

	it.currentBlock = nil
	blockReader := bytes.NewReader(decompressedBlock)
	var prevKey shared.Key

	for blockReader.Len() > 0 {
		var lcp, suffixLen uint16
		binary.Read(blockReader, binary.LittleEndian, &lcp)
		binary.Read(blockReader, binary.LittleEndian, &suffixLen)

		suffix := make([]byte, suffixLen)
		io.ReadFull(blockReader, suffix)

		currentKey := make(shared.Key, lcp+suffixLen)
		copy(currentKey, prevKey[:lcp])
		copy(currentKey[lcp:], suffix)

		var valLen uint32
		binary.Read(blockReader, binary.LittleEndian, &valLen)
		value := make([]byte, valLen)
		io.ReadFull(blockReader, value)

		var tombstone bool
		binary.Read(blockReader, binary.LittleEndian, &tombstone)

		entry := shared.Entry{
			Key:       currentKey,
			Value:     value,
			Tombstone: tombstone,
		}

		it.currentBlock = append(it.currentBlock, entry)
		prevKey = currentKey
	}

	it.entryIdx = 0
	return nil
}

func (it *SSTableIterator) next() (*shared.Entry, error) {
	if it.finished {
		return nil, nil
	}

	if it.entryIdx >= len(it.currentBlock) {
		it.blockIdx++
		err := it.loadCurrentBlock()
		if err != nil {
			return nil, err
		}
		if it.finished {
			return nil, nil
		}
	}

	if it.entryIdx < len(it.currentBlock) {
		entry := &it.currentBlock[it.entryIdx]
		it.entryIdx++
		return entry, nil
	}

	return nil, nil
}

func (it *SSTableIterator) close() {
}

func compact(outputPath string, first *SSTable, second *SSTable, deleteZombie bool, config *SSTableConfig) (*SSTable, error) {
	firstIterator, err := first.newIterator()
	if err != nil {
		return nil, err
	}
	defer firstIterator.close()

	err = firstIterator.seekStart()
	if err != nil {
		return nil, err
	}

	secondIterator, err := second.newIterator()
	if err != nil {
		return nil, err
	}
	defer secondIterator.close()

	err = secondIterator.seekStart()
	if err != nil {
		return nil, err
	}

	writer, err := NewBlockWriter(outputPath, config)
	if err != nil {
		return nil, err
	}

	currentFirstEntry, err := firstIterator.next()
	if err != nil {
		return nil, err
	}

	currentSecondEntry, err := secondIterator.next()
	if err != nil {
		return nil, err
	}

	entryCount := 0

	for currentFirstEntry != nil && currentSecondEntry != nil {
		if bytes.Equal(currentFirstEntry.Key, currentSecondEntry.Key) {
			if !(currentSecondEntry.Tombstone && deleteZombie) {
				err = writer.Add(*currentSecondEntry)
				if err != nil {
					return nil, err
				}
				entryCount++
			}
			currentFirstEntry, err = firstIterator.next()
			if err != nil {
				return nil, err
			}
			currentSecondEntry, err = secondIterator.next()
			if err != nil {
				return nil, err
			}
		} else if bytes.Compare(currentFirstEntry.Key, currentSecondEntry.Key) < 0 {
			if !(currentFirstEntry.Tombstone && deleteZombie) {
				err = writer.Add(*currentFirstEntry)
				if err != nil {
					return nil, err
				}
				entryCount++
			}
			currentFirstEntry, err = firstIterator.next()
			if err != nil {
				return nil, err
			}
		} else {
			if !(currentSecondEntry.Tombstone && deleteZombie) {
				err = writer.Add(*currentSecondEntry)
				if err != nil {
					return nil, err
				}
				entryCount++
			}
			currentSecondEntry, err = secondIterator.next()
			if err != nil {
				return nil, err
			}
		}
	}

	for currentFirstEntry != nil {
		if !(currentFirstEntry.Tombstone && deleteZombie) {
			err = writer.Add(*currentFirstEntry)
			if err != nil {
				return nil, err
			}
			entryCount++
		}
		currentFirstEntry, err = firstIterator.next()
		if err != nil {
			return nil, err
		}
	}

	for currentSecondEntry != nil {
		if !(currentSecondEntry.Tombstone && deleteZombie) {
			err = writer.Add(*currentSecondEntry)
			if err != nil {
				return nil, err
			}
			entryCount++
		}
		currentSecondEntry, err = secondIterator.next()
		if err != nil {
			return nil, err
		}
	}

	err = writer.Finish()
	if err != nil {
		return nil, err
	}

	if entryCount == 0 {
		return nil, nil
	}

	return Open(outputPath)
}
