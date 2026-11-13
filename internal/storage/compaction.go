package storage

import (
	"bytes"
	"container/heap"

	"github.com/rs/zerolog/log"
)

type iteratorNode struct {
	it         *SSTableIterator
	generation int // Higher means newer (e.g., 0 is oldest)
}

type iteratorHeap []*iteratorNode

func (h iteratorHeap) Len() int { return len(h) }
func (h iteratorHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].it.Rec.Key, h[j].it.Rec.Key)
	if cmp == 0 {
		// Identical keys, pop the newer generation first
		return h[i].generation > h[j].generation
	}
	return cmp < 0
}
func (h iteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *iteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(*iteratorNode))
}

func (h *iteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

func Compact(sstFiles []*SSTableRef, dataDir string, isOldestLevel bool) (string, error) {
	if len(sstFiles) == 0 {
		return "", nil
	}

	h := &iteratorHeap{}
	heap.Init(h)

	var estimatedTotalItems int
	var iterators []*SSTableIterator

	// Open all iterators and push the first record into the heap
	for generation, ref := range sstFiles {
		filePath := ref.FilePath
		it, err := NewSSTableIterator(filePath)
		if err != nil {
			// Cleanup previously opened iterators
			for _, opened := range iterators {
				opened.Close()
			}
			return "", err
		}
		iterators = append(iterators, it)

		count, err := ReadSSTableKeyCount(filePath)
		if err != nil {
			estimatedTotalItems += 5000
		} else {
			estimatedTotalItems += int(count)
		}

		if it.Next() {
			heap.Push(h, &iteratorNode{
				it:         it,
				generation: generation,
			})
		}
	}

	defer func() {
		for _, it := range iterators {
			it.Close()
		}
	}()

	writer, err := NewSSTableWriter(dataDir, estimatedTotalItems)
	if err != nil {
		return "", err
	}

	var currentKey []byte

	for h.Len() > 0 {
		// Pop the iterator with the smallest key (and newest generation if tied)
		node := heap.Pop(h).(*iteratorNode)

		isDuplicate := false
		if currentKey != nil && bytes.Equal(currentKey, node.it.Rec.Key) {
			isDuplicate = true
		}
		currentKey = append([]byte(nil), node.it.Rec.Key...)

		if !isDuplicate {
			// If it's a tombstone and we are compacting the oldest level, drop it!
			if node.it.Rec.IsTombstone && isOldestLevel {
			} else {
				err := writer.Add(node.it.Rec.Key, node.it.Rec.Value, node.it.Rec.IsTombstone)
				if err != nil {
					log.Error().Err(err).Msg("Compaction write failed")
					return "", err
				}
			}
		}

		if node.it.Next() {
			heap.Push(h, node)
		}
	}

	return writer.Close()
}
