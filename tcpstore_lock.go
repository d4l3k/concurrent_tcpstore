package main

import (
	"sync"
	"fmt"
)

type LockStore struct {
    mu struct{
		sync.RWMutex

		store map[string][]byte
		conds map[string]*sync.Cond
	}
}
var _ Store = (*LockStore)(nil)


func NewLockStore() *LockStore {
	store := &LockStore{}
	store.mu.store = make(map[string][]byte)
	store.mu.conds = make(map[string]*sync.Cond)
	return store
}

func (s *LockStore) Set(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.mu.store[key] = value

	cond, ok := s.mu.conds[key]
	if ok {
		cond.Broadcast()
	}

	return nil
}

func (s *LockStore) Get(key string) ([]byte, error) {
	s.mu.RLock()
	value, ok := s.mu.store[key]
	s.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("key %s not found", key)
	}

	return value, nil
}

func (s *LockStore) Add(key string, incr int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	bytes, ok := s.mu.store[key]
	var v int64 = 0
	if ok {
		v = bytesToInt64(bytes)
	}
	v += incr
	s.mu.store[key] = int64ToBytes(v)

	return v, nil
}

func (s *LockStore) Wait(keys []string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, key := range keys {
		for _, ok := s.mu.store[key]; !ok; {

			cond, ok := s.mu.conds[key]
			if !ok {
				// promote to write lock
				s.mu.RUnlock()
				s.mu.Lock()
				cond, ok = s.mu.conds[key]
				if !ok {
					cond = sync.NewCond(&s.mu)
					s.mu.conds[key] = cond
				}

				// downgrade to read lock
				s.mu.Unlock()
				s.mu.RLock()
			}

			cond.Wait()
		}
	}

	return nil
}
