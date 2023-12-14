package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

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
		snap: []byte("{}"),
		vclock: make(map[string]uint64),
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

	// merge of the diff
	ans := []transaction{}

	// get merge of transactions that are present in our journal
	// NOTICE: we stop early when we cover all the diferences
	done := make(map[string]bool)
	out:
	for i := len(s.journal) - 1; i >= 0; i-- {
		tr := s.journal[i]

		if knownId, idOk := from[tr.Source]; idOk {
			if knownId < tr.Id {
				ans = append(ans, tr)
			} else {
				done[tr.Source] = true

				for s := range from {
					if done[s] != true {
						continue out
					}
				}
				break
			}
		}
 	}

	for i, j := 0, len(ans)-1; i < j; i, j = i+1, j-1 {
        ans[i], ans[j] = ans[j], ans[i]
    }

	// decode current snap
	var cur_snap map[string]any
	uErr := json.Unmarshal(s.snap, &cur_snap)
	if uErr != nil {
		log.Printf("Cannot unmarshall snap: %v", uErr)
		return []transaction{}, uErr
	}

	// create transactions for all the sources incoming clock is not aware of
	for key, val := range cur_snap {
		if _, key_ok := from[key]; !key_ok {
			jsonVal, mErr := json.Marshal(val)
			if mErr != nil {
				log.Printf("Cannot marshal new value for patch: %v", mErr)
				continue
			}
			newPatch := transaction{
				Source:  key,
				Id:      s.vclock[key],
				Payload: fmt.Sprintf("[{\"op\": \"add\", \"path\": \"/%v\", \"value\": %v}]", key, string(jsonVal)),
			}
			ans = append(ans, newPatch)
		}
	}

	return ans, nil
}

func (s *TManager) run(in <-chan transaction) error {
	loop:
	for t := range in {
		log.Printf("Working on transaction: %v", t)

		sourceTime, sourceOk := s.vclock[t.Source]
		if sourceOk && sourceTime > t.Id {
			continue loop
		}
		rawPatch := []byte(t.Payload)
		patch, decErr := jsonpatch.DecodePatch(rawPatch)
		if decErr != nil {
			log.Printf("Decode patch err: %v", decErr)
			continue loop
		}

		if !sourceOk {
			addCommand := json.RawMessage("\"add\"")
			pathCommand := json.RawMessage("\"/" + t.Source + "\"")
			valueCommand := json.RawMessage("\"\"")
			patch = append(
				[]jsonpatch.Operation{
					map[string]*json.RawMessage{
						"op": &addCommand,
						"path": &pathCommand,
						"value": &valueCommand,
					},
				},
				([]jsonpatch.Operation)(patch)...
			)
		}

		s.mutex.Lock()
		{
			log.Printf("Applying patch: %v", string(rawPatch))
			modified, modErr := patch.Apply((*s).snap)
			if modErr != nil {
				s.mutex.Unlock()
				log.Printf("Apply patch err: %v", modErr)
				continue loop
			}

			(*s).snap = modified
			(*s).journal = append((*s).journal, t)
			(*s).vclock[t.Source] = t.Id
		}
		s.mutex.Unlock()
	}

	return nil
}
