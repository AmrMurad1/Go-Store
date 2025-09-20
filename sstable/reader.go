package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sort"

	"github.com/AmrMurad1/Go-Store/shared"
)

type SSTable struct {
	file         *os.File
	indexRecords []shared.IndexRecord
	meta         shared.MetaBlock
	footer       shared.Footer
}

func Open(filename string) (*SSTable, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	sstable := &SSTable{
		file: file,
	}

	//read footer
	footerBytes := make([]byte, shared.FooterSize)
	if _, err := file.ReadAt(footerBytes, stat.Size()-int64(shared.FooterSize)); err != nil {
		return nil, err
	}
	footerReader := bytes.NewReader(footerBytes)
	if err := binary.Read(footerReader, binary.LittleEndian, &sstable.footer); err != nil {
		return nil, err
	}

	if sstable.footer.Magic != shared.MagicNumber {
		return nil, errors.New("invalid sstable file: magic number mismatch")
	}

	indexBytes := make([]byte, sstable.footer.IndexBlockSize)
	if _, err := file.ReadAt(indexBytes, sstable.footer.IndexBlockOffset); err != nil {
		return nil, err
	}
	indexReader := bytes.NewReader(indexBytes)
	for indexReader.Len() > 0 {
		var keyLen uint32
		if err := binary.Read(indexReader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		key := make([]byte, keyLen)
		if _, err := io.ReadFull(indexReader, key); err != nil {
			return nil, err
		}
		var record shared.IndexRecord
		record.LastKey = shared.Key(key)
		if err := binary.Read(indexReader, binary.LittleEndian, &record.Offset); err != nil {
			return nil, err
		}
		if err := binary.Read(indexReader, binary.LittleEndian, &record.Size); err != nil {
			return nil, err
		}
		sstable.indexRecords = append(sstable.indexRecords, record)
	}

	metaBytes := make([]byte, sstable.footer.MetaBlockSize)
	if _, err := file.ReadAt(metaBytes, sstable.footer.MetaBlockOffset); err != nil {
		return nil, err
	}
	metaReader := bytes.NewReader(metaBytes)
	if err := binary.Read(metaReader, binary.LittleEndian, &sstable.meta.EntryCount); err != nil {
		return nil, err
	}
	var minKeyLen, maxKeyLen uint32
	if err := binary.Read(metaReader, binary.LittleEndian, &minKeyLen); err != nil {
		return nil, err
	}
	minKey := make([]byte, minKeyLen)
	if _, err := io.ReadFull(metaReader, minKey); err != nil {
		return nil, err
	}
	sstable.meta.MinKey = shared.Key(minKey)
	if err := binary.Read(metaReader, binary.LittleEndian, &maxKeyLen); err != nil {
		return nil, err
	}
	maxKey := make([]byte, maxKeyLen)
	if _, err := io.ReadFull(metaReader, maxKey); err != nil {
		return nil, err
	}
	sstable.meta.MaxKey = shared.Key(maxKey)
	if err := binary.Read(metaReader, binary.LittleEndian, &sstable.meta.Timestamp); err != nil {
		return nil, err
	}

	return sstable, nil
}

func (s *SSTable) Get(key shared.Key) (*shared.Entry, error) {
	if key < s.meta.MinKey || key > s.meta.MaxKey {
		return nil, nil
	}

	indexRecordIndex := sort.Search(len(s.indexRecords), func(i int) bool {
		return s.indexRecords[i].LastKey >= key
	})

	if indexRecordIndex == len(s.indexRecords) {
		return nil, nil
	}

	record := s.indexRecords[indexRecordIndex]
	dataBlockBytes := make([]byte, record.Size)
	_, err := s.file.ReadAt(dataBlockBytes, record.Offset)
	if err != nil {
		return nil, err
	}

	blockReader := bytes.NewReader(dataBlockBytes)
	for blockReader.Len() > 0 {
		var keyLen, valLen uint32
		var tombstone bool

		if err := binary.Read(blockReader, binary.LittleEndian, &keyLen); err != nil {
			return nil, err
		}
		currentKeyBytes := make([]byte, keyLen)
		if _, err := io.ReadFull(blockReader, currentKeyBytes); err != nil {
			return nil, err
		}
		currentKey := shared.Key(currentKeyBytes)

		if err := binary.Read(blockReader, binary.LittleEndian, &valLen); err != nil {
			return nil, err
		}
		value := make([]byte, valLen)
		if _, err := io.ReadFull(blockReader, value); err != nil {
			return nil, err
		}

		if err := binary.Read(blockReader, binary.LittleEndian, &tombstone); err != nil {
			return nil, err
		}

		if currentKey == key {
			return &shared.Entry{
				Key:       currentKey,
				Value:     value,
				Tombstone: tombstone,
			}, nil
		}
	}

	return nil, nil
}

func (s *SSTable) Close() error {
	return s.file.Close()
}
