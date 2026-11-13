package storage

import (
	"errors"
	"os"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

var (
	ErrNotFound = errors.New("not found")
	ErrEmptyKey = errors.New("empty key")
)

type MemTable struct {
	mu sync.RWMutex

	active    *SkipList
	immutable []*SkipList
	sstables  []*SSTableRef // List of on-disk SSTable file references (oldest first)
	capacity  int

	dataDir   string
	flushChan chan *SkipList
	quitChan  chan struct{}
	wg        sync.WaitGroup
}

func NewMemTable(capacity int) *MemTable {
	dataDir := os.Getenv("VORTEXKV_DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}

	m := &MemTable{
		active:    NewSkipList(capacity),
		immutable: make([]*SkipList, 0),
		sstables:  make([]*SSTableRef, 0),
		capacity:  capacity,
		dataDir:   dataDir,
		flushChan: make(chan *SkipList, 10),
		quitChan:  make(chan struct{}),
	}

	m.wg.Add(2)
	go m.flushWorker()
	go m.compactionWorker()

	return m
}

func (m *MemTable) flushWorker() {
	defer m.wg.Done()
	for {
		select {
		case sl := <-m.flushChan:
			m.processFlush(sl)
		case <-m.quitChan:
			for {
				select {
				case sl := <-m.flushChan:
					m.processFlush(sl)
				default:
					return
				}
			}
		}
	}
}

func (m *MemTable) processFlush(sl *SkipList) {
	log.Debug().Msg("Starting flush process for memtable")
	filePath, err := flushSkipListToSSTable(sl, m.dataDir)
	if err != nil {
		log.Error().Err(err).Msg("Failed to flush SSTable")
		return
	}

	log.Info().Str("filePath", filePath).Msg("Flushed SSTable")

	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, imm := range m.immutable {
		if imm == sl {
			idx = i
			break
		}
	}

	if idx != -1 {
		copy(m.immutable[idx:], m.immutable[idx+1:])
		m.immutable[len(m.immutable)-1] = nil
		m.immutable = m.immutable[:len(m.immutable)-1]
	}

	// Append to sstables list (oldest first)
	m.sstables = append(m.sstables, NewSSTableRef(filePath))
}

func (m *MemTable) compactionWorker() {
	defer m.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.tryCompaction()
		case <-m.quitChan:
			return
		}
	}
}

func (m *MemTable) tryCompaction() {
	m.mu.RLock()
	sstCount := len(m.sstables)
	log.Debug().Int("sstCount", sstCount).Msg("Checking compaction conditions")

	if sstCount < 4 {
		m.mu.RUnlock()
		return
	}

	filesToCompact := make([]*SSTableRef, 4)
	copy(filesToCompact, m.sstables[:4])
	m.mu.RUnlock()

	log.Info().Msg("Triggering compaction for 4 oldest SSTables...")

	newSSTable, err := Compact(filesToCompact, m.dataDir, true)
	if err != nil {
		log.Error().Err(err).Msg("Compaction failed")
		return
	}

	log.Info().Str("newSSTable", newSSTable).Msg("Compaction successful")

	m.mu.Lock()
	if len(m.sstables) >= 4 {
		copy(m.sstables[1:], m.sstables[4:])
		m.sstables[0] = NewSSTableRef(newSSTable) // Index 0 is oldest
		m.sstables = m.sstables[:len(m.sstables)-3]
	}
	m.mu.Unlock()

	for _, oldRef := range filesToCompact {
		oldRef.MarkDeleted()
		log.Info().Str("filePath", oldRef.FilePath).Msg("Marked old compacted file for deletion")
	}
}

func (m *MemTable) put(key []byte, value []byte) error {
	err := m.active.Put(key, value)
	if err == ErrSkipListFull {
		oldActive := m.active
		m.immutable = append(m.immutable, oldActive)
		m.active = NewSkipList(m.capacity)

		m.flushChan <- oldActive

		err = m.active.Put(key, value)
	}
	return err
}

func (m *MemTable) Put(key []byte, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if value == nil {
		value = []byte{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.put(key, value)
}

func (m *MemTable) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	var sstablesCopy []*SSTableRef
	sstablesCopy = append(sstablesCopy, m.sstables...)

	// Acquire references to prevent deletion during read
	for _, ref := range sstablesCopy {
		ref.Acquire()
	}

	val, err := m.active.Get(key)
	if err == nil {
		m.mu.RUnlock()
		if val == nil {
			return nil, ErrNotFound
		}
		return val, nil
	}

	for i := len(m.immutable) - 1; i >= 0; i-- {
		val, err := m.immutable[i].Get(key)
		if err == nil {
			m.mu.RUnlock()
			if val == nil {
				return nil, ErrNotFound
			}
			return val, nil
		}
	}

	m.mu.RUnlock()

	defer func() {
		for _, ref := range sstablesCopy {
			ref.Release()
		}
	}()

	for i := len(sstablesCopy) - 1; i >= 0; i-- {
		sstPath := sstablesCopy[i].FilePath
		val, err := SearchSSTable(sstPath, key)
		if err == nil {
			return val, nil
		}
		if err != ErrNotFound {
			log.Warn().Err(err).Str("sstPath", sstPath).Msg("Error reading SSTable")
		}
	}

	return nil, ErrNotFound
}

func (m *MemTable) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	return m.put(key, nil)
}

func (m *MemTable) SizeBytes() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	size := m.active.SizeBytes()
	for _, imm := range m.immutable {
		size += imm.SizeBytes()
	}
	return size
}

func (m *MemTable) Close() error {
	close(m.quitChan)
	m.wg.Wait()
	return nil
}
