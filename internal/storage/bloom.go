package storage

import (
	"github.com/spaolacci/murmur3"
)

type BloomFilter struct {
	bits []byte
	k    uint8
}

// Using ~10 bits per item gives a ~1% false positive rate with k=7.
func NewBloomFilter(expectedItems int) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 100
	}

	numBits := expectedItems * 10
	numBytes := (numBits + 7) / 8

	return &BloomFilter{
		bits: make([]byte, numBytes),
		k:    7,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := getHashes(key)
	numBits := uint32(len(bf.bits) * 8)

	for i := uint8(0); i < bf.k; i++ {
		h := h1 + uint32(i)*h2
		bitPos := h % numBits

		byteIdx := bitPos / 8
		bitIdx := bitPos % 8
		bf.bits[byteIdx] |= (1 << bitIdx)
	}
}

func (bf *BloomFilter) MayContain(key []byte) bool {
	if len(bf.bits) == 0 {
		return false
	}

	h1, h2 := getHashes(key)
	numBits := uint32(len(bf.bits) * 8)

	for i := uint8(0); i < bf.k; i++ {
		h := h1 + uint32(i)*h2
		bitPos := h % numBits

		byteIdx := bitPos / 8
		bitIdx := bitPos % 8

		if (bf.bits[byteIdx] & (1 << bitIdx)) == 0 {
			return false
		}
	}

	return true
}

func (bf *BloomFilter) Encode() []byte {
	// Format: [k (1 byte)][bits...]
	encoded := make([]byte, 1+len(bf.bits))
	encoded[0] = bf.k
	copy(encoded[1:], bf.bits)
	return encoded
}

func DecodeBloomFilter(data []byte) *BloomFilter {
	if len(data) < 1 {
		return &BloomFilter{bits: []byte{}, k: 0}
	}
	return &BloomFilter{
		k:    data[0],
		bits: data[1:],
	}
}

func getHashes(key []byte) (uint32, uint32) {
	hash64 := murmur3.Sum64(key)
	h1 := uint32(hash64)
	h2 := uint32(hash64 >> 32)
	return h1, h2
}
