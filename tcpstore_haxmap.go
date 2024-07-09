package main

import (
	"sync"
	"fmt"

	"github.com/alphadose/haxmap"
)

type Entry struct{
	mu struct{
		sync.RWMutex

		cond *sync.Cond

		valid bool
		data []byte
	}
}

type ConcurrentStore struct{
	store *haxmap.Map[string, *Entry]
}

var _ Store = (*ConcurrentStore)(nil)

func NewConcurrentStore() *ConcurrentStore {
	store := &ConcurrentStore{}
	store.store = haxmap.New[string, *Entry]()
	return store
}

func (s *ConcurrentStore) getOrCreateEntry(key string) *Entry {
	v, _ := s.store.GetOrCompute(key, func() *Entry {
		entry := &Entry{}
		entry.mu.cond = sync.NewCond(entry.mu.RLocker())
		return entry
	})
	return v
}

func (s *ConcurrentStore) Get(key string) ([]byte, error) {
	entry, ok := s.store.Get(key)
	if !ok {
		return nil, fmt.Errorf("key %s not found", key)
	}

	entry.mu.RLock()
	defer entry.mu.RUnlock()

	return entry.mu.data, nil
}

func (s *ConcurrentStore) Set(key string, data []byte) error {
	entry := s.getOrCreateEntry(key)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.mu.data = data
	entry.mu.valid = true
	entry.mu.cond.Broadcast()
	return nil
}

func (s *ConcurrentStore) Wait(keys []string) (error) {
	for _, key := range keys {
		entry := s.getOrCreateEntry(key)
		entry.mu.RLock()

		for !entry.mu.valid {
			entry.mu.cond.Wait()
		}

		entry.mu.RUnlock()
	}

	return nil
}

func (s *ConcurrentStore) Add(key string, incr int64) (int64, error) {
	entry := s.getOrCreateEntry(key)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	var v int64 = 0
	if entry.mu.valid {
		v = bytesToInt64(entry.mu.data)
	}
	v += incr
	entry.mu.data = int64ToBytes(v)
	entry.mu.valid = true

	return v, nil
}
