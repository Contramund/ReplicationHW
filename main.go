package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
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

		// timeout to save network resources
		time.Sleep(1 * time.Second)

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

	// Setup logger
	logFile, oErr := os.OpenFile(fmt.Sprintf("%s_%s.log", addr, nickname), os.O_CREATE | os.O_APPEND | os.O_RDWR, 0666)
	if oErr != nil {
		log.Fatal("Cannot initialize basic logging file: ", oErr)
	}
	mw := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(mw)

	fmt.Printf("Starting with nickname \"%v\" connecting to ports:\n", nickname)
	for peer := range peers {
		fmt.Printf("-> %v", peer)
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