package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"os"
	"time"

	"github.com/AmrMurad1/Go-Store/shared"
	"github.com/klauspost/compress/s2"
)

type BlockWriter struct {
	file          *os.File
	writer        *bufio.Writer
	config        *SSTableConfig
	dataBlockBuf  bytes.Buffer
	indexRecords  []shared.IndexRecord
	meta          shared.MetaBlock
	filter        *Filter
	currentOffset int64
	entryCounter  uint64
	prevKey       shared.Key
}

type SSTableConfig struct {
	DataBlockSize           int
	FilterFalsePositiveRate float64
	ExpectedEntryCount      int
}

func NewBlockWriter(filename string, config *SSTableConfig) (*BlockWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	return &BlockWriter{
		file:   file,
		writer: bufio.NewWriter(file),
		config: config,
		meta: shared.MetaBlock{
			Timestamp: time.Now().UnixNano(),
		},
		filter: New(config.ExpectedEntryCount, config.FilterFalsePositiveRate),
	}, nil
}

func (bw *BlockWriter) Add(entry shared.Entry) error {
	if bw.entryCounter == 0 {
		bw.meta.MinKey = entry.Key
	}
	bw.meta.MaxKey = entry.Key
	bw.entryCounter++
	bw.filter.Add(string(entry.Key))

	lcp := lcp(bw.prevKey, entry.Key)
	suffix := entry.Key[lcp:]

	binary.Write(&bw.dataBlockBuf, binary.LittleEndian, uint16(lcp))
	binary.Write(&bw.dataBlockBuf, binary.LittleEndian, uint16(len(suffix)))
	bw.dataBlockBuf.Write([]byte(suffix))
	binary.Write(&bw.dataBlockBuf, binary.LittleEndian, uint32(len(entry.Value)))
	bw.dataBlockBuf.Write(entry.Value)
	binary.Write(&bw.dataBlockBuf, binary.LittleEndian, entry.Tombstone)

	bw.prevKey = entry.Key

	if bw.dataBlockBuf.Len() >= bw.config.DataBlockSize {
		if err := bw.flushDataBlock(); err != nil {
			return err
		}
	}
	return nil
}

func (bw *BlockWriter) flushDataBlock() error {
	if bw.dataBlockBuf.Len() == 0 {
		return nil
	}

	compressedBlock := s2.Encode(nil, bw.dataBlockBuf.Bytes())
	n, err := bw.writer.Write(compressedBlock)
	if err != nil {
		return err
	}

	bw.indexRecords = append(bw.indexRecords, shared.IndexRecord{
		LastKey: bw.prevKey,
		Offset:  bw.currentOffset,
		Size:    int32(n),
	})

	bw.currentOffset += int64(n)
	bw.dataBlockBuf.Reset()
	return nil
}

func (bw *BlockWriter) Finish() error {
	if err := bw.flushDataBlock(); err != nil {
		return err
	}

	bw.meta.EntryCount = bw.entryCounter

	filterOffset := bw.currentOffset
	filterBytes := bw.filter.Encode()
	if _, err := bw.writer.Write(filterBytes); err != nil {
		return err
	}
	bw.currentOffset += int64(len(filterBytes))

	metaBlockOffset := bw.currentOffset
	metaBuf := new(bytes.Buffer)
	binary.Write(metaBuf, binary.LittleEndian, bw.meta.EntryCount)
	binary.Write(metaBuf, binary.LittleEndian, uint32(len(bw.meta.MinKey)))
	metaBuf.Write([]byte(bw.meta.MinKey))
	binary.Write(metaBuf, binary.LittleEndian, uint32(len(bw.meta.MaxKey)))
	metaBuf.Write([]byte(bw.meta.MaxKey))
	binary.Write(metaBuf, binary.LittleEndian, bw.meta.Timestamp)
	metaBlockBytes := metaBuf.Bytes()
	if _, err := bw.writer.Write(metaBlockBytes); err != nil {
		return err
	}
	bw.currentOffset += int64(len(metaBlockBytes))

	// write index block
	indexBlockOffset := bw.currentOffset
	indexBuf := new(bytes.Buffer)
	for _, record := range bw.indexRecords {
		binary.Write(indexBuf, binary.LittleEndian, uint32(len(record.LastKey)))
		indexBuf.Write([]byte(record.LastKey))
		binary.Write(indexBuf, binary.LittleEndian, record.Offset)
		binary.Write(indexBuf, binary.LittleEndian, record.Size)
	}
	indexBlockBytes := indexBuf.Bytes()
	if _, err := bw.writer.Write(indexBlockBytes); err != nil {
		return err
	}
	bw.currentOffset += int64(len(indexBlockBytes))

	// Write footer
	footer := shared.Footer{
		FilterOffset:     filterOffset,
		FilterSize:       uint32(len(filterBytes)),
		MetaBlockOffset:  metaBlockOffset,
		MetaBlockSize:    uint32(len(metaBlockBytes)),
		IndexBlockOffset: indexBlockOffset,
		IndexBlockSize:   uint32(len(indexBlockBytes)),
		Magic:            shared.MagicNumber,
	}
	footerBuf := new(bytes.Buffer)
	binary.Write(footerBuf, binary.LittleEndian, &footer)
	if _, err := bw.writer.Write(footerBuf.Bytes()); err != nil {
		return err
	}

	if err := bw.writer.Flush(); err != nil {
		return err
	}
	return bw.file.Close()
}

func lcp(a, b shared.Key) int {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	return i
}
