package main

import (
	_ "embed"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

var (
	//go:embed index.html
	indexFile []byte
)

type replicationServer struct {
	serveMux http.ServeMux
	nickname string
}

func (cs *replicationServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	cs.serveMux.ServeHTTP(w, r)
}

func getTestHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write(indexFile)
	}
}

func getVclockHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		pureAns := tm.getVClock()
		jsonAns, mErr := json.Marshal(pureAns)
		if mErr != nil {
			rw.WriteHeader(400)
		} else {
			rw.WriteHeader(http.StatusOK)
			rw.Header().Set("Content-Type", "application/json")
			rw.Write(jsonAns)
		}
	}
}

func getPostHandler(tm *TManager, pipe chan<- transaction, nickname string) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		localTime := tm.getVClock()[nickname]
		patch, rErr := io.ReadAll(req.Body)
		if rErr != nil {
			log.Printf("Cannot read post body: %v", rErr)
			rw.WriteHeader(400)
			return
		}
		pipe <- transaction{
			Source:  nickname,
			Id:      localTime + 1,
			Payload: string(patch),
		}
		rw.WriteHeader(http.StatusOK)
	}
}

func getGetHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(tm.getSnap()) // getSnap couldn't fail
	}
}

func getWsHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		pureVClock := req.Header.Get("VClock")
		var VClockIn map[string]uint64

		if pureVClock == "" {
			rw.WriteHeader(200)
			rw.Header().Set("Content-Type", "application/json")
			rw.Write([]byte("{}"))
		}

		umErr := json.Unmarshal([]byte(pureVClock), &VClockIn)
		if umErr != nil {
			log.Printf("Cannot unmarshal ws clock: %v", umErr)
			rw.WriteHeader(400)
			return
		}

		pureAns, diffErr := tm.getDiff(VClockIn)
		if diffErr != nil {
			log.Printf("Cannot find diff log: %v", diffErr)
			rw.WriteHeader(400)
			return
		}

		jsonAns, mErr := json.Marshal(pureAns)
		if mErr != nil {
			log.Printf("Cannot marshall ans: %v", mErr)
			rw.WriteHeader(400)
			return
		}

		rw.WriteHeader(200)
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(jsonAns)
	}
}

func newReplicationServer(tm *TManager, tmPipe chan<- transaction, nickname string) *replicationServer {
	rs := &replicationServer {
		nickname: nickname,
	}

	rs.serveMux.HandleFunc("/test", getTestHandler(tm))
	rs.serveMux.HandleFunc("/vclock", getVclockHandler(tm))
	rs.serveMux.HandleFunc("/post", getPostHandler(tm, tmPipe, nickname))
	rs.serveMux.HandleFunc("/get", getGetHandler(tm))
	rs.serveMux.HandleFunc("/ws", getWsHandler(tm))

	return rs
}