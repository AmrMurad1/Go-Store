package memtable

import (
	"math/rand"
	"time"
	"unsafe"

	"github.com/AmrMurad1/Go-Store/shared"
)

const _head = "HEAD"

type SkipList struct {
	maxLevel int
	p        float64
	level    int
	rand     *rand.Rand
	size     int
	head     *Element
}

type Element struct {
	shared.Entry
	next []*Element
}

func New(maxLevel int, p float64) *SkipList {
	return &SkipList{
		maxLevel: maxLevel,
		p:        p,
		level:    1,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		size:     0,
		head: &Element{
			Entry: shared.Entry{
				Key:       shared.Key(_head),
				Value:     nil,
				Tombstone: false,
				Version:   0,
			},
			next: make([]*Element, maxLevel),
		},
	}
}

func (s *SkipList) Size() int {
	return s.size
}

func (s *SkipList) Set(entry shared.Entry) int {
	curr := s.head
	update := make([]*Element, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && shared.CompareKeys(curr.next[i].Key, entry.Key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// update entry
	if curr.next[0] != nil && shared.CompareKeys(curr.next[0].Key, entry.Key) == 0 {
		sizeChange := len(entry.Value) - len(curr.next[0].Value)
		s.size += sizeChange
		curr.next[0].Value = entry.Value
		curr.next[0].Tombstone = entry.Tombstone
		return sizeChange
	}

	// add entry
	level := s.randomLevel()
	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	e := &Element{
		Entry: shared.Entry{
			Key:       entry.Key,
			Value:     entry.Value,
			Tombstone: entry.Tombstone,
			Version:   entry.Version,
		},
		next: make([]*Element, level),
	}

	for i := 0; i < level; i++ {
		e.next[i] = update[i].next[i]
		update[i].next[i] = e
	}

	sizeChange := len(entry.Key) + len(entry.Value) +
		int(unsafe.Sizeof(entry.Tombstone)) +
		int(unsafe.Sizeof(entry.Version)) +
		len(e.next)*int(unsafe.Sizeof((*Element)(nil)))
	s.size += sizeChange
	return sizeChange
}

func (s *SkipList) Get(key shared.Key) (shared.Entry, bool) {
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && shared.CompareKeys(curr.next[i].Key, key) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]

	if curr != nil && shared.CompareKeys(curr.Key, key) == 0 {
		return shared.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		}, true
	}
	return shared.Entry{}, false
}

func (s *SkipList) LowerBound(key shared.Key) (shared.Entry, bool) {
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && shared.CompareKeys(curr.next[i].Key, key) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]

	if curr != nil {
		return shared.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		}, true
	}
	return shared.Entry{}, false
}

func (s *SkipList) Scan(start, end shared.Key) []shared.Entry {
	var res []shared.Entry
	curr := s.head
	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && shared.CompareKeys(curr.next[i].Key, start) < 0 {
			curr = curr.next[i]
		}
	}
	curr = curr.next[0]

	for curr != nil && shared.CompareKeys(curr.Key, end) < 0 {
		res = append(res, shared.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		})
		curr = curr.next[0]
	}
	return res
}

func (s *SkipList) All() []shared.Entry {
	var all []shared.Entry
	for curr := s.head.next[0]; curr != nil; curr = curr.next[0] {
		all = append(all, shared.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		})
	}
	return all
}

func (s *SkipList) randomLevel() int {
	level := 1
	for s.rand.Float64() < s.p && level < s.maxLevel {
		level++
	}
	return level
}
