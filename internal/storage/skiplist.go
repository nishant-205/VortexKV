package storage

import (
	"bytes"
	"errors"
	"math/rand"
	"time"
)

const maxLevel = 32
const p = 0.5

var ErrSkipListFull = errors.New("skip list is at capacity")

type node struct {
	key     []byte
	value   []byte
	forward []*node
}

func newNode(key, value []byte, level int) *node {
	return &node{
		key:     key,
		value:   value,
		forward: make([]*node, level),
	}
}

type SkipList struct {
	header    *node
	level     int
	sizeBytes int
	capacity  int
	randGen   *rand.Rand
}

func NewSkipList(capacity int) *SkipList {
	source := rand.NewSource(time.Now().UnixNano())
	randGen := rand.New(source)

	return &SkipList{
		header:   newNode(nil, nil, maxLevel),
		level:    1,
		capacity: capacity,
		randGen:  randGen,
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for sl.randGen.Float32() < p && level < maxLevel {
		level++
	}
	return level
}

func (sl *SkipList) Put(key, value []byte) error {
	update := make([]*node, maxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	if current != nil && bytes.Equal(current.key, key) {
		sizeDiff := len(value) - len(current.value)
		if sl.sizeBytes+sizeDiff > sl.capacity {
			return ErrSkipListFull
		}
		sl.sizeBytes += sizeDiff
		current.value = value
		return nil
	}

	newSize := len(key) + len(value)
	if sl.sizeBytes+newSize > sl.capacity {
		return ErrSkipListFull
	}

	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.header
		}
		sl.level = newLevel
	}

	n := newNode(key, value, newLevel)
	for i := 0; i < newLevel; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}

	sl.sizeBytes += newSize
	return nil
}

func (sl *SkipList) Get(key []byte) ([]byte, error) {
	current := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		return current.value, nil
	}
	return nil, ErrNotFound
}

func (sl *SkipList) Delete(key []byte) error {
	update := make([]*node, maxLevel)
	current := sl.header

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && bytes.Compare(current.forward[i].key, key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]
	if current != nil && bytes.Equal(current.key, key) {
		for i := 0; i < sl.level; i++ {
			if update[i].forward[i] != current {
				break
			}
			update[i].forward[i] = current.forward[i]
		}

		for sl.level > 1 && sl.header.forward[sl.level-1] == nil {
			sl.level--
		}
		sl.sizeBytes -= len(current.key) + len(current.value)
		return nil
	}
	return ErrNotFound
}

func (sl *SkipList) SizeBytes() int {
	return sl.sizeBytes
}

func (sl *SkipList) IsFull() bool {
	return sl.sizeBytes >= sl.capacity
}

func (sl *SkipList) ForEach(fn func(key, value []byte)) {
	current := sl.header.forward[0]
	for current != nil {
		fn(current.key, current.value)
		current = current.forward[0]
	}
}
