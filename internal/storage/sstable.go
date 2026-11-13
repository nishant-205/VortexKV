package storage

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------
// SSTable Reference Counting
// ---------------------------------------------------------

type SSTableRef struct {
	FilePath string
	refCount atomic.Int32
	deleted  atomic.Bool
}

func NewSSTableRef(path string) *SSTableRef {
	return &SSTableRef{FilePath: path}
}

func (r *SSTableRef) Acquire() {
	r.refCount.Add(1)
}

func (r *SSTableRef) Release() {
	if r.refCount.Add(-1) == 0 && r.deleted.Load() {
		os.Remove(r.FilePath)
	}
}

func (r *SSTableRef) MarkDeleted() {
	r.deleted.Store(true)
	if r.refCount.Load() == 0 {
		os.Remove(r.FilePath)
	}
}

const (
	BlockSize   = 4096                       // 4KB blocks
	MagicNumber = uint64(0x0000000056525458) // "VRTX"
)

type IndexEntry struct {
	LastKey []byte
	Offset  uint32
	Size    uint32
}

func generateSSTableName() string {
	now := time.Now().UnixNano()
	randBytes := make([]byte, 4)
	_, _ = rand.Read(randBytes)
	hash := hex.EncodeToString(randBytes)
	return fmt.Sprintf("sst_%d_%s.sst", now, hash)
}

// ---------------------------------------------------------
// SSTable Writer
// ---------------------------------------------------------

type SSTableWriter struct {
	file          *os.File
	filePath      string
	bf            *BloomFilter
	index         []IndexEntry
	currentOffset uint32
	blockBuf      *bytes.Buffer
	lastKey       []byte
	writeErr      error
	keyCount      uint32
}

func NewSSTableWriter(dataDir string, expectedItems int) (*SSTableWriter, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	fileName := generateSSTableName()
	filePath := filepath.Join(dataDir, fileName)

	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable file: %w", err)
	}

	return &SSTableWriter{
		file:     file,
		filePath: filePath,
		bf:       NewBloomFilter(expectedItems),
		blockBuf: new(bytes.Buffer),
	}, nil
}

func (w *SSTableWriter) Add(key, value []byte, isTombstone bool) error {
	if w.writeErr != nil {
		return w.writeErr
	}

	w.keyCount++
	w.bf.Add(key)

	tempBuf := new(bytes.Buffer)
	tombstoneByte := byte(0)
	if isTombstone || value == nil {
		tombstoneByte = byte(1)
	}

	binary.Write(tempBuf, binary.LittleEndian, tombstoneByte)
	binary.Write(tempBuf, binary.LittleEndian, uint32(len(key)))
	tempBuf.Write(key)

	valLen := uint32(0)
	if !isTombstone && value != nil {
		valLen = uint32(len(value))
	}
	binary.Write(tempBuf, binary.LittleEndian, valLen)
	if valLen > 0 {
		tempBuf.Write(value)
	}

	if w.blockBuf.Len() > 0 && w.blockBuf.Len()+tempBuf.Len() > BlockSize {
		blockSize := uint32(w.blockBuf.Len())
		if _, err := w.file.Write(w.blockBuf.Bytes()); err != nil {
			w.writeErr = err
			return err
		}

		w.index = append(w.index, IndexEntry{
			LastKey: append([]byte(nil), w.lastKey...),
			Offset:  w.currentOffset,
			Size:    blockSize,
		})

		w.currentOffset += blockSize
		w.blockBuf.Reset()
	}

	w.blockBuf.Write(tempBuf.Bytes())
	w.lastKey = key
	return nil
}

func (w *SSTableWriter) Close() (string, error) {
	if w.writeErr != nil {
		w.file.Close()
		return "", w.writeErr
	}

	if w.blockBuf.Len() > 0 {
		blockSize := uint32(w.blockBuf.Len())
		if _, err := w.file.Write(w.blockBuf.Bytes()); err != nil {
			w.file.Close()
			return "", err
		}
		w.index = append(w.index, IndexEntry{
			LastKey: append([]byte(nil), w.lastKey...),
			Offset:  w.currentOffset,
			Size:    blockSize,
		})
		w.currentOffset += blockSize
	}

	// Write Index Block
	indexOffset := w.currentOffset
	indexBuf := new(bytes.Buffer)
	binary.Write(indexBuf, binary.LittleEndian, uint32(len(w.index)))
	for _, entry := range w.index {
		binary.Write(indexBuf, binary.LittleEndian, uint32(len(entry.LastKey)))
		indexBuf.Write(entry.LastKey)
		binary.Write(indexBuf, binary.LittleEndian, entry.Offset)
		binary.Write(indexBuf, binary.LittleEndian, entry.Size)
	}
	if _, err := w.file.Write(indexBuf.Bytes()); err != nil {
		w.file.Close()
		return "", err
	}
	indexSize := uint32(indexBuf.Len())
	w.currentOffset += indexSize

	// Write Bloom Filter Block
	bloomOffset := w.currentOffset
	bloomData := w.bf.Encode()
	if _, err := w.file.Write(bloomData); err != nil {
		w.file.Close()
		return "", err
	}
	bloomSize := uint32(len(bloomData))

	// Write Footer
	footerBuf := new(bytes.Buffer)
	binary.Write(footerBuf, binary.LittleEndian, indexOffset)
	binary.Write(footerBuf, binary.LittleEndian, indexSize)
	binary.Write(footerBuf, binary.LittleEndian, bloomOffset)
	binary.Write(footerBuf, binary.LittleEndian, bloomSize)
	binary.Write(footerBuf, binary.LittleEndian, w.keyCount)
	binary.Write(footerBuf, binary.LittleEndian, MagicNumber)

	if _, err := w.file.Write(footerBuf.Bytes()); err != nil {
		w.file.Close()
		return "", err
	}

	w.file.Close()
	return w.filePath, nil
}

func flushSkipListToSSTable(sl *SkipList, dataDir string) (string, error) {
	estimatedItems := sl.SizeBytes() / 20
	w, err := NewSSTableWriter(dataDir, estimatedItems)
	if err != nil {
		return "", err
	}

	sl.ForEach(func(key, value []byte) {
		w.Add(key, value, value == nil)
	})

	return w.Close()
}

// ---------------------------------------------------------
// SSTable Search
// ---------------------------------------------------------

func SearchSSTable(filePath string, searchKey []byte) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := stat.Size()

	if fileSize < 28 {
		return nil, fmt.Errorf("file too small to be a valid SSTable")
	}

	_, err = file.Seek(-28, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	footerBytes := make([]byte, 28)
	if _, err := io.ReadFull(file, footerBytes); err != nil {
		return nil, err
	}

	footerBuf := bytes.NewReader(footerBytes)
	var indexOffset, indexSize, bloomOffset, bloomSize, keyCount uint32
	var magic uint64

	binary.Read(footerBuf, binary.LittleEndian, &indexOffset)
	binary.Read(footerBuf, binary.LittleEndian, &indexSize)
	binary.Read(footerBuf, binary.LittleEndian, &bloomOffset)
	binary.Read(footerBuf, binary.LittleEndian, &bloomSize)
	binary.Read(footerBuf, binary.LittleEndian, &keyCount)
	binary.Read(footerBuf, binary.LittleEndian, &magic)

	if magic != MagicNumber {
		return nil, fmt.Errorf("invalid magic number, not an SSTable")
	}

	_, err = file.Seek(int64(bloomOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	bloomBytes := make([]byte, bloomSize)
	if _, err := io.ReadFull(file, bloomBytes); err != nil {
		return nil, err
	}

	bf := DecodeBloomFilter(bloomBytes)
	if !bf.MayContain(searchKey) {
		return nil, ErrNotFound
	}

	_, err = file.Seek(int64(indexOffset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	indexBytes := make([]byte, indexSize)
	if _, err := io.ReadFull(file, indexBytes); err != nil {
		return nil, err
	}

	indexBuf := bytes.NewReader(indexBytes)
	var numEntries uint32
	binary.Read(indexBuf, binary.LittleEndian, &numEntries)

	var index []IndexEntry
	for i := uint32(0); i < numEntries; i++ {
		var keyLen uint32
		binary.Read(indexBuf, binary.LittleEndian, &keyLen)

		key := make([]byte, keyLen)
		io.ReadFull(indexBuf, key)

		var offset, size uint32
		binary.Read(indexBuf, binary.LittleEndian, &offset)
		binary.Read(indexBuf, binary.LittleEndian, &size)

		index = append(index, IndexEntry{
			LastKey: key,
			Offset:  offset,
			Size:    size,
		})
	}

	blockIdx := sort.Search(len(index), func(i int) bool {
		return bytes.Compare(index[i].LastKey, searchKey) >= 0
	})

	if blockIdx >= len(index) {
		return nil, ErrNotFound
	}

	targetBlock := index[blockIdx]

	_, err = file.Seek(int64(targetBlock.Offset), io.SeekStart)
	if err != nil {
		return nil, err
	}
	blockBytes := make([]byte, targetBlock.Size)
	if _, err := io.ReadFull(file, blockBytes); err != nil {
		return nil, err
	}

	blockReader := bytes.NewReader(blockBytes)
	for blockReader.Len() > 0 {
		var isTombstone byte
		var keyLen uint32

		binary.Read(blockReader, binary.LittleEndian, &isTombstone)
		binary.Read(blockReader, binary.LittleEndian, &keyLen)

		k := make([]byte, keyLen)
		io.ReadFull(blockReader, k)

		var valLen uint32
		binary.Read(blockReader, binary.LittleEndian, &valLen)

		v := make([]byte, valLen)
		if valLen > 0 {
			io.ReadFull(blockReader, v)
		}

		cmp := bytes.Compare(k, searchKey)
		if cmp == 0 {
			if isTombstone == 1 {
				return nil, ErrNotFound
			}
			return v, nil
		} else if cmp > 0 {
			break
		}
	}

	return nil, ErrNotFound
}

func ReadSSTableKeyCount(filePath string) (uint32, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return 0, err
	}
	if stat.Size() < 28 {
		return 0, fmt.Errorf("file too small to be a valid SSTable")
	}

	_, err = file.Seek(-28, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	footerBytes := make([]byte, 28)
	if _, err := io.ReadFull(file, footerBytes); err != nil {
		return 0, err
	}

	footerBuf := bytes.NewReader(footerBytes)
	var indexOffset, indexSize, bloomOffset, bloomSize, keyCount uint32
	var magic uint64

	binary.Read(footerBuf, binary.LittleEndian, &indexOffset)
	binary.Read(footerBuf, binary.LittleEndian, &indexSize)
	binary.Read(footerBuf, binary.LittleEndian, &bloomOffset)
	binary.Read(footerBuf, binary.LittleEndian, &bloomSize)
	binary.Read(footerBuf, binary.LittleEndian, &keyCount)
	binary.Read(footerBuf, binary.LittleEndian, &magic)

	if magic != MagicNumber {
		return 0, fmt.Errorf("invalid magic number, not an SSTable")
	}

	return keyCount, nil
}

// ---------------------------------------------------------
// SSTable Iterator
// ---------------------------------------------------------

type Record struct {
	Key         []byte
	Value       []byte
	IsTombstone bool
}

type SSTableIterator struct {
	file        *os.File
	indexOffset int64
	offset      int64
	Rec         *Record
	Err         error
}

func NewSSTableIterator(filePath string) (*SSTableIterator, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	_, err = file.Seek(-28, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, err
	}
	footerBytes := make([]byte, 28)
	if _, err := io.ReadFull(file, footerBytes); err != nil {
		file.Close()
		return nil, err
	}

	footerBuf := bytes.NewReader(footerBytes)
	var indexOffset uint32
	binary.Read(footerBuf, binary.LittleEndian, &indexOffset)

	_, err = file.Seek(0, io.SeekStart)
	if err != nil {
		file.Close()
		return nil, err
	}

	return &SSTableIterator{
		file:        file,
		indexOffset: int64(indexOffset),
		offset:      0,
	}, nil
}

func (it *SSTableIterator) Next() bool {
	if it.Err != nil || it.offset >= it.indexOffset {
		it.Rec = nil
		return false
	}

	var isTombstone byte
	var keyLen uint32

	if err := binary.Read(it.file, binary.LittleEndian, &isTombstone); err != nil {
		it.Err = err
		it.Rec = nil
		return false
	}
	if err := binary.Read(it.file, binary.LittleEndian, &keyLen); err != nil {
		it.Err = err
		it.Rec = nil
		return false
	}

	key := make([]byte, keyLen)
	if _, err := io.ReadFull(it.file, key); err != nil {
		it.Err = err
		it.Rec = nil
		return false
	}

	var valLen uint32
	if err := binary.Read(it.file, binary.LittleEndian, &valLen); err != nil {
		it.Err = err
		it.Rec = nil
		return false
	}

	var val []byte
	if valLen > 0 {
		val = make([]byte, valLen)
		if _, err := io.ReadFull(it.file, val); err != nil {
			it.Err = err
			it.Rec = nil
			return false
		}
	}

	it.offset += int64(1 + 4 + keyLen + 4 + valLen)
	it.Rec = &Record{
		Key:         key,
		Value:       val,
		IsTombstone: isTombstone == 1,
	}

	return true
}

func (it *SSTableIterator) Close() {
	if it.file != nil {
		it.file.Close()
	}
}
