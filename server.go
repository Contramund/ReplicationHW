package main

import (
	_ "embed"
	"encoding/json"
	"net/http"
)

var (
	//go:embed index.html
	indexFile []byte
)

type replicationServer struct {
	serveMux http.ServeMux
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

func getPostHandler(pipe chan<- transaction) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		// TODO
	}
}

func getGetHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(tm.getSnap()) // getSnap couldn't fail
	}
}

func getWsHandler(pipe chan<- transaction) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		// TODO
	}
}

func newReplicationServer(tm *TManager, tmPipe chan<- transaction) *replicationServer {
	rs := &replicationServer {}

	rs.serveMux.HandleFunc("/test", getTestHandler(tm))
	rs.serveMux.HandleFunc("/vclock", getVclockHandler(tm))
	rs.serveMux.HandleFunc("/post", getPostHandler(tmPipe))
	rs.serveMux.HandleFunc("/get", getGetHandler(tm))
	rs.serveMux.HandleFunc("/ws", getWsHandler(tmPipe))

	return rs
}