package shared

type Key string

type Entry struct {
	Key       Key
	Value     []byte
	Tombstone bool
	Version   int
}

func CompareKeys(k1, k2 Key) int {
	if k1 < k2 {
		return -1
	} else if k1 > k2 {
		return 1
	}
	return 0
}
