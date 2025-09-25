package filter

import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

type Filter struct {
	bitset  []bool
	hashFns []hash.Hash32
}

func New(n int, p float64) *Filter {
	if n <= 0 || p <= 0 || p >= 1 {
		return nil
	}

	m := int(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
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

// Add adds a key to the bloom filter
func (f *Filter) Add(key string) {
	for _, fn := range f.hashFns {
		_, _ = fn.Write([]byte(key))
		index := int(fn.Sum32()) % len(f.bitset)
		f.bitset[index] = true
		fn.Reset()
	}
}

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

// Decode deserializes a byte slice into a new Bloom Filter.
func Decode(data []byte) (*Filter, error) {
	f := &Filter{
		bitset: make([]bool, len(data)*8),
	}
	for i := 0; i < len(f.bitset); i++ {
		if (data[i/8] & (1 << (i % 8))) != 0 {
			f.bitset[i] = true
		}
	}
	return f, nil
}
