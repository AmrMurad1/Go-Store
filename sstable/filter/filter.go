package filter
package filter

import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

// Filter is a Bloom Filter implementation.
type Filter struct {
	bitset  []bool
	hashFns []hash.Hash32
}

// New creates a new BloomFilter with an optimal size based on the expected
// number of elements (n) and the desired false positive rate (p).
func New(n int, p float64) *Filter {
	if n <= 0 || p <= 0 || p >= 1 {
		// Return a nil filter or handle error appropriately if params are invalid
		return nil
	}

	// m = -(n * ln(p)) / (ln(2)^2)
	m := int(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	// k = (m/n) * ln(2)
	k := int(math.Round((float64(m) / float64(n)) * math.Log(2)))

	if m == 0 || k == 0 {
		return nil
	}

	hashFns := make([]hash.Hash32, k)
	for i := 0; i < k; i++ {
		hashFns[i] = murmur3.New32WithSeed(uint32(i))
	}

	return &Filter{
		bitset:  make([]bool, m),
		hashFns: hashFns,
	}
}

// Add adds a key to the Bloom Filter.
func (f *Filter) Add(key string) {
	for _, fn := range f.hashFns {
		_, _ = fn.Write([]byte(key))
		index := int(fn.Sum32()) % len(f.bitset)
		f.bitset[index] = true
		fn.Reset()
	}
}

// Contains checks if a key is possibly in the set.
// It returns false if the key is definitely not in the set.
// It returns true if the key might be in the set (with a certain false positive probability).
func (f *Filter) Contains(key string) bool {
	for _, fn := range f.hashFns {
		_, _ = fn.Write([]byte(key))
		index := int(fn.Sum32()) % len(f.bitset)
		fn.Reset()
		if !f.bitset[index] {
			return false
		}
	}
	return true
}

// Encode serializes the Bloom Filter's bitset to a byte slice.
func (f *Filter) Encode() []byte {
	buf := make([]byte, (len(f.bitset)+7)/8)
	for i, b := range f.bitset {
		if b {
			buf[i/8] |= 1 << (i % 8)
		}
	}
	return buf
}

// Decode deserializes a byte slice into a Bloom Filter's bitset.
// Note: This assumes the Filter was already created with the correct size and hash functions.
func (f *Filter) Decode(data []byte) {
	for i := 0; i < len(f.bitset); i++ {
		if (data[i/8] & (1 << (i % 8))) != 0 {
			f.bitset[i] = true
		}
	}
}
