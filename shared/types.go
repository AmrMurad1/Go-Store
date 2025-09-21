package shared

import "bytes"

type Key []byte

func (k Key) Compare(other Key) int {
	return bytes.Compare(k, other)
}

type Entry struct {
	Key       Key
	Value     []byte
	Tombstone bool
	Version   int
}

func CompareKeys(k1, k2 Key) int {
	return bytes.Compare(k1, k2)
}
