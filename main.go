package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"
	"nhooyr.io/websocket"
)

func runReplicationClient(ctx context.Context, pipe chan<- transaction, peer string) {
	for {
		c, resp, err := websocket.Dial(ctx, peer, nil)
		if err != nil {
			log.Printf("Cannot connect to %v", peer)
			time.Sleep(3 * time.Second)
			c.CloseNow()
		}

		// process resp
		body, bErr := io.ReadAll(resp.Body)
		if bErr != nil {
			log.Printf("Cannot read body: %v", bErr)
			c.CloseNow()
			continue
		}
		var updates []transaction
		umErr := json.Unmarshal(body, &updates)
		if umErr != nil {
			log.Printf("Cannot unmarshall body: %v", umErr)
			c.CloseNow()
			continue
		}

		for _, tr := range updates {
			pipe <- tr
		}

		c.CloseNow()
	}
}

func main() {
	// Parse CMD options
	addr := ""
	peers := []string{}

	// Init transaction manager
	tm, tmErr := NewTManager()
	if tmErr != nil {
		log.Fatal(tmErr)
	}
	tmPipe := make(chan transaction, 10)
	go tm.run(tmPipe)

	// Setup replication client
	ctxWithCancel, cancelClient := context.WithCancel(context.Background())
	defer cancelClient()
	for _, peer := range peers {
		go runReplicationClient(ctxWithCancel, tmPipe, peer)
	}

	// Setup replication server
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on http://%v", l.Addr())

	rs := newReplicationServer(&tm, tmPipe)
	s := &http.Server{
		Handler:      rs,
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- s.Serve(l)
	}()


	// Setup graceful shutdown
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)

	select {
	case err := <-serverErr:
		log.Printf("failed to serve: %v", err)
	case sig := <-sigs:
		log.Printf("terminating: %v", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	s.Shutdown(ctx)
}