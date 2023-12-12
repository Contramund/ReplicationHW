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
	ansMap := make(map[string]transaction)

	// get merge of transactions that are present in our journal
	// NOTICE: we stop early when we cover all the diferences
	done := make(map[string]bool)
	out:
	for i := len(s.journal) - 1; i >= 0; i-- {
		tr := s.journal[i]

		if knownId, idOk := from[tr.Source]; idOk {
			if knownId < tr.Id {
				if oldVal, ok := ansMap[tr.Source]; ok {
					newPatch, pErr := jsonpatch.MergeMergePatches([]byte(tr.Payload), []byte(oldVal.Payload))
					if pErr != nil {
						log.Printf("Cannot merge patches: %v", pErr)
						return []transaction{}, pErr
					}
					ansMap[tr.Source] = transaction{
						Source:  tr.Source,
						Id:      oldVal.Id,
						Payload: string(newPatch),
					}
				} else {
					ansMap[tr.Source] = tr
				}
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

	// decode current snap
	var cur_snap map[string]string
	uErr := json.Unmarshal(s.snap, &cur_snap)
	if uErr != nil {
		log.Printf("Cannot unmarshall snap: %v", uErr)
		return []transaction{}, uErr
	}

	// create transactions for all the sources incoming clock is not aware of
	for key, val := range cur_snap {
		if _, key_ok := from[key]; !key_ok {
			newPatch, pErr := jsonpatch.CreateMergePatch([]byte("{}"), []byte(val))
			if pErr != nil {
				log.Printf("Cannot create patch: %v", pErr)
				return []transaction{}, pErr
			}
			ansMap[key] = transaction{
				Source:  key,
				Id:      s.vclock[key],
				Payload: string(newPatch),
			}
		}
	}

	ansList := []transaction{}
	for _, val := range ansMap {
		ansList = append(ansList, val)
	}

	return ansList, nil
}

func (s *TManager) run(in <-chan transaction) error {
	loop:
	for t := range in {
		log.Printf("Working on transaction: %v", t)
		sourceTime, sourceOk := s.vclock[t.Source]
		if sourceOk && sourceTime > t.Id {
			continue loop
		}

		var rawPatch []byte
		// fix transaction for non-yet-existent client
		if !sourceOk {
			// createPatch, cErr := jsonpatch.CreateMergePatch([]byte("{}"), []byte(fmt.Sprintf("{\"%v\": \"\"}", t.Source)))
			// if cErr != nil {
			// 	log.Printf("Create merge patch err: %v", cErr)
			// 	continue loop
			// }
			patch1 := []byte(fmt.Sprintf("[{ \"op\": \"add\" , \"path\": \"/%v\" , \"value\": \"\" }]", t.Source))
			patch2 := []byte(fmt.Sprintf("[{ \"op\": \"replace\" , \"path\": \"/%v\" , \"value\": \"lol\" }]", t.Source))
			var pErr error = nil
			rawPatch, pErr = jsonpatch.MergeMergePatches(patch1, patch2)
			if pErr != nil {
				log.Printf("Merge merge patch err: %v", pErr)
				continue loop
			}
			log.Printf("get patch: %v", string(rawPatch))
		} else {
			rawPatch = []byte(t.Payload)
		}

		log.Printf("get patch: %v", string(rawPatch))

		patch, decErr := jsonpatch.DecodePatch(rawPatch)
		if decErr != nil {
			log.Printf("Decode patch err: %v", decErr)
			continue loop
		}

		s.mutex.Lock()
		{
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
