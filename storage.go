package main

import (
	"sync"
	// "encoding/json"
	jsonpatch "github.com/evanphx/json-patch/v5"
)


type TManager struct {
	journal []transaction
	snap []byte
	vclock map[string]uint64
	mutex sync.RWMutex
}


func NewTManager() (TManager, error) {
	return TManager {
		journal: []transaction{},
		snap: []byte{},
		vclock: make(map[string]uint64, 0),
		mutex: sync.RWMutex{},
	}, nil
}

func (s *TManager) getSnap() []byte {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.snap
}

func (s *TManager) getVClock() map[string]uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.vclock
}

func (s *TManager) getDiff(from map[string]uint64) ([]transaction, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// TODO: get diff
	return []transaction{}, nil
}

func (s *TManager) run(in <-chan transaction) error {
	for t := range in {
		if s.vclock[t.Source] > t.Id {
			continue
		}

		patch, decErr := jsonpatch.DecodePatch([]byte(t.Payload))
		if decErr != nil {
			return decErr
		}

		s.mutex.Lock()
		{
			modified, modErr := patch.Apply((*s).snap)
			if modErr != nil {
				s.mutex.Unlock()
				return modErr
			}

			(*s).snap = modified
			(*s).journal = append((*s).journal, t)
			(*s).vclock[t.Source] = t.Id
		}
		s.mutex.Unlock()
	}

	return nil
}
