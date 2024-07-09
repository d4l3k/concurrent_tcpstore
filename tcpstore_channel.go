package main

import (
	"fmt"
	"encoding/binary"
)

type response struct{
	Str []byte
	Int uint64

	Err error
}
type request struct{
	Query QueryType
	Key string
	Value []byte
	Incr uint64

	Response chan response
}
type entry struct{
	Value []byte
	Waiters []chan response
}

type ChannelStore struct {
	reqs chan request

	// this should only be accessed by worker()
	kv map[string]entry
}
var _ Store = (*ChannelStore)(nil)


func NewChannelStore() *ChannelStore {
	s := &ChannelStore{
		reqs: make(chan request, 1024),
		kv: make(map[string]entry),
	}
	go s.worker()
	return s
}

func (s *ChannelStore) worker() {
	kv := s.kv
	for req := range s.reqs {
		switch req.Query {
		case GET:
			e, ok := kv[req.Key]
			if !ok {
				req.Response <- response{Err: fmt.Errorf("key not found %s", req.Key)}
				continue
			}
			req.Response <- response{
				Str: e.Value,
			}

		case SET:
			e := kv[req.Key]
			for _, waiter := range e.Waiters {
				waiter <- response{}
			}
			e.Waiters = nil
			e.Value = req.Value
			kv[req.Key] = e

		case WAIT:
			e, ok := kv[req.Key]
			if !ok || len(e.Waiters) > 0 {
				e.Waiters = append(e.Waiters, req.Response)
				kv[req.Key] = e
			} else {
				req.Response <- response{}
			}

		case ADD:
			v := uint64(0)
			e, ok := kv[req.Key]
			if ok {
				v = binary.LittleEndian.Uint64(e.Value)

				for _, waiter := range e.Waiters {
					waiter <- response{}
				}
				e.Waiters = nil
			} else {
				e.Value = make([]byte, 8)
			}
			v += req.Incr
			binary.LittleEndian.PutUint64(e.Value, v)

			req.Response <- response{
				Int: v,
			}

		default:
			req.Response <- response{Err: fmt.Errorf("unknown query %v", req.Query)}
		}
	}
}

func (s *ChannelStore) Set(key string, value []byte) error {
	respC := make(chan response, 1)
	s.reqs <- request{
		Query: SET,
		Key: key,
		Value: value,
		Response: respC,
	}

	return nil
}

func (s *ChannelStore) Get(key string) ([]byte, error) {
	respC := make(chan response, 1)
	s.reqs <- request{
		Query: GET,
		Key: key,
		Response: respC,
	}

	resp := <- respC
	return []byte(resp.Str), resp.Err
}

func (s *ChannelStore) Add(key string, incr int64) (int64, error) {
	if incr < 0 {
		return 0, fmt.Errorf("incr must be positive")
	}

	respC := make(chan response, 1)
	s.reqs <- request{
		Query: ADD,
		Key: key,
		Incr: uint64(incr),
		Response: respC,
	}

	resp := <- respC
	return int64(resp.Int), resp.Err
}

func (s *ChannelStore) waitKey(key string) error {
	respC := make(chan response, 1)
	s.reqs <- request{
		Query: WAIT,
		Key: key,
		Response: respC,
	}

	resp := <- respC
	return resp.Err
}

func (s *ChannelStore) Wait(keys []string) error {
	for _, key := range keys {
		if err := s.waitKey(key); err != nil {
			return err
		}
	}

	return nil
}
