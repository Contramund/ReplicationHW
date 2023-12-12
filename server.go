package main

import (
	_ "embed"
	"encoding/json"
	"io"
	"log"
	"net/http"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

var (
	//go:embed index.html
	indexFile []byte
)

type replicationServer struct {
	serveMux http.ServeMux
	nickname string
}

func logRequest(req *http.Request, route string) {
	log.Printf("Incoming command on route '%v'.\nHeaders: %v.\nBody: %v\n\n", route, req.Header, req.Body)
}

func (rs *replicationServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("Incoming request: %v", r.Method)
	rs.serveMux.ServeHTTP(w, r)
}

// [{"op":"replace", "path":"Contramund","value": []}]
func getTestHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/test")
		rw.WriteHeader(http.StatusOK)
		rw.Write(indexFile)
	}
}

func getVclockHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/vclock")
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
		logRequest(req, "/replace")
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
		logRequest(req, "/get")
		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(tm.getSnap()) // getSnap couldn't fail
	}
}

func getWsHandler(tm *TManager) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/ws")

		c, aErr := websocket.Accept(rw, req, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
			OriginPatterns:     []string{"*"},
		})
		if aErr != nil {
			log.Printf("Error accept socket: %v", aErr)
			rw.WriteHeader(400)
			return
		}


		pureVClock := req.Header.Get("VClock")
		var VClockIn map[string]uint64

		if pureVClock == "" {
			pureVClock = "{}"
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

		log.Printf("WS ans: \"%v\"", jsonAns)

		wsjson.Write(req.Context(), c, pureAns)

		rw.WriteHeader(200)
		// rw.Header().Set("Content-Type", "application/json")
		// rw.Write(jsonAns)
	}
}

func newReplicationServer(tm *TManager, tmPipe chan<- transaction, nickname string) *replicationServer {
	rs := &replicationServer {
		nickname: nickname,
	}

	rs.serveMux.HandleFunc("/test", getTestHandler(tm))
	rs.serveMux.HandleFunc("/vclock", getVclockHandler(tm))
	rs.serveMux.HandleFunc("/replace", getPostHandler(tm, tmPipe, nickname))
	rs.serveMux.HandleFunc("/get", getGetHandler(tm))
	rs.serveMux.HandleFunc("/ws", getWsHandler(tm))

	return rs
}