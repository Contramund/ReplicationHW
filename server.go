package main

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

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
	logger *log.Logger
}

func logRequest(req *http.Request, route string, logger *log.Logger) {
	logger.Printf("Incoming command on route '%v'.\nHeaders: %v.\nBody: %v\n\n", route, req.Header, req.Body)
}

func (rs *replicationServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rs.logger.Printf("Incoming request: %v\n\n", r)
	rs.serveMux.ServeHTTP(w, r)
}

// [{"op":"replace", "path":"Contramund","value": []}]
func getTestHandler(tm *TManager, logger *log.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/test", logger)
		rw.WriteHeader(http.StatusOK)
		rw.Write(indexFile)
	}
}

func getVclockHandler(tm *TManager, logger *log.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/vclock", logger)

		pureAns := tm.getVClock()
		jsonAns, mErr := json.Marshal(pureAns)
		if mErr != nil {
			logger.Printf("Cannot marshal vclock: %v\n\n", mErr)
			rw.WriteHeader(400)
		} else {
			rw.WriteHeader(http.StatusOK)
			rw.Header().Set("Content-Type", "application/json")
			rw.Write(jsonAns)
		}
	}
}

func getReplaceHandler(tm *TManager, pipe chan<- transaction, nickname string, logger *log.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/replace", logger)

		localTime, timeOk := tm.getVClock()[nickname]
		if !timeOk {
			localTime = 0
		}
		patch, rErr := io.ReadAll(req.Body)
		if rErr != nil {
			logger.Printf("Cannot read /replace body: %v\n\n", rErr)
			rw.WriteHeader(400)
			return
		}

		// check if updates are OK
		var jsonPatch []map[string]*json.RawMessage
		uErr := json.Unmarshal(patch, &jsonPatch)
		if uErr != nil {
			log.Printf("Cannot unmarshall /replace body: %v", uErr)
			rw.WriteHeader(400)
			return
		}
		for num, m := range jsonPatch {
			if !bytes.HasPrefix(*m["path"], []byte("\"/" + nickname)) {
				log.Printf("Cannot accept patch: update number %v should starts with \"/%v\" in patch \"%v\"", num + 1, nickname, string(patch))
				rw.WriteHeader(400)
				return
			}
		}

		pipe <- transaction{
			Source:  nickname,
			Id:      localTime + 1,
			Payload: string(patch),
		}
		rw.WriteHeader(http.StatusOK)
	}
}

func getGetHandler(tm *TManager, logger *log.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/get", logger)

		rw.WriteHeader(http.StatusOK)
		rw.Header().Set("Content-Type", "application/json")
		rw.Write(tm.getSnap()) // getSnap couldn't fail
	}
}

func getWsHandler(tm *TManager, logger *log.Logger) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		logRequest(req, "/ws", logger)

		c, aErr := websocket.Accept(rw, req, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
			OriginPatterns:     []string{"*"},
		})
		if aErr != nil {
			logger.Printf("Cannot accept /ws handshake: %v\n\n", aErr)
			rw.WriteHeader(400)
			return
		}

		pureVClock := req.Header.Get("VClock")
		var VClockIn map[string]uint64

		// handle index.html queries
		if pureVClock == "" {
			tm.mutex.RLock()
			journal := make([]transaction, len(tm.journal))
			copy(journal, tm.journal)
			tm.mutex.RUnlock()
			wsjson.Write(req.Context(), c, journal)
			return
		}

		umErr := json.Unmarshal([]byte(pureVClock), &VClockIn)
		if umErr != nil {
			logger.Printf("Cannot unmarshal ws clock: %v\n\n", umErr)
			rw.WriteHeader(400)
			return
		}

		pureAns, diffErr := tm.getDiff(VClockIn)
		if diffErr != nil {
			logger.Printf("Cannot find diff log: %v\n\n", diffErr)
			rw.WriteHeader(400)
			return
		}

		wsjson.Write(req.Context(), c, pureAns)
	}
}

func newReplicationServer(tm *TManager, tmPipe chan<- transaction, nickname string) *replicationServer {
	// init server's logger
	logFile, oErr := os.OpenFile(fmt.Sprintf("server_%s.log", nickname), os.O_CREATE | os.O_APPEND | os.O_RDWR, 0666)
	if oErr != nil {
		log.Fatal("Cannot initialize basic logging file: ", oErr)
	}
	logger := log.New(logFile, "", log.Default().Flags())

	// init server itself
	rs := &replicationServer {
		nickname: nickname,
		logger: logger,
	}

	// setup routes&handlers
	rs.serveMux.HandleFunc("/test", getTestHandler(tm, logger))
	rs.serveMux.HandleFunc("/vclock", getVclockHandler(tm, logger))
	rs.serveMux.HandleFunc("/replace", getReplaceHandler(tm, tmPipe, nickname, logger))
	rs.serveMux.HandleFunc("/get", getGetHandler(tm, logger))
	rs.serveMux.HandleFunc("/ws", getWsHandler(tm, logger))

	return rs
}