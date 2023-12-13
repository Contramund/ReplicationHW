package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"time"

	"nhooyr.io/websocket"
)

func runReplicationClient(ctx context.Context, tm *TManager, pipe chan<- transaction, peer string) {
	loop:
	for {
		select {
		case end := <- ctx.Done():
			log.Printf("Ending replication client '%v' due to context: %v", peer, end)
			break loop
		default:
		}

		time.Sleep(3 * time.Second)

		vclockIn, mErr := json.Marshal(tm.getVClock())
		if mErr != nil {
			log.Printf("Cannot marshall vclock: %v", mErr)
			continue
		}

		opts := websocket.DialOptions{
			HTTPHeader: http.Header{"VClock": []string{string(vclockIn)}},
		}
		c, _, dErr := websocket.Dial(ctx, "ws://" + peer + "/ws", &opts)
		if dErr != nil {
			log.Printf("Cannot connect to %v: %v", peer, dErr)
			time.Sleep(3 * time.Second)
			// c.CloseNow()
			continue
		}

		_, body, rErr := c.Read(ctx)
		if rErr != nil {
			log.Printf("Cannot read from websocket %v: %v", peer, rErr)
			time.Sleep(3 * time.Second)
			// c.CloseNow()
			continue
		}

		var updates []transaction
		umErr := json.Unmarshal(body, &updates)
		if umErr != nil {
			log.Printf("Cannot unmarshall body: %v", umErr)
			// c.CloseNow()
			continue
		}

		log.Printf("Got updates: %v", updates)
		for _, tr := range updates {
			pipe <- tr
		}

		// c.CloseNow()
	}
}

func main() {
	// Parse CMD options
	addrPtr := flag.String("p", "localhost:8080", "set a port for server to run on")
	nicknamePtr := flag.String("n", "Contramund", "set a name for your server in the system")

	flag.Parse()

	addr := *addrPtr
	nickname := *nicknamePtr
	peers := flag.Args()

	log.Printf("Starting with nickname \"%v\" connecting to ports:\n", nickname)
	for peer := range peers {
		log.Printf("-> %v", peer)
	}

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
		go runReplicationClient(ctxWithCancel, &tm, tmPipe, peer)
	}

	// Setup replication server
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on http://%v", l.Addr())

	rs := newReplicationServer(&tm, tmPipe, nickname)
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