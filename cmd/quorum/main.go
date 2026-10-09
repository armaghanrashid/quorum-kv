// Command quorum runs one node of the replicated key-value store over HTTP.
//
// Start one process per entry in --peers, each with its own --id and --data:
//
//	quorum --id 0 --data /tmp/q0 --peers localhost:7000,localhost:7001,localhost:7002
//	quorum --id 1 --data /tmp/q1 --peers localhost:7000,localhost:7001,localhost:7002
//	quorum --id 2 --data /tmp/q2 --peers localhost:7000,localhost:7001,localhost:7002
//
// Then talk to whichever node is the leader:
//
//	curl -X PUT  localhost:7000/kv/greeting -d hello
//	curl         localhost:7000/kv/greeting
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/armaghanrashid/quorum-kv/kv"
	"github.com/armaghanrashid/quorum-kv/raft"
	"github.com/armaghanrashid/quorum-kv/transport"
)

func main() {
	id := flag.Int("id", 0, "this node's index into --peers")
	peerList := flag.String("peers", "", "comma-separated host:port of every node, ordered by id")
	dataDir := flag.String("data", "", "directory for durable Raft state (required)")
	snapEvery := flag.Uint64("snapshot-every", 1000, "snapshot after this many applied entries (0 disables)")
	flag.Parse()

	if err := run(*id, *peerList, *dataDir, *snapEvery); err != nil {
		fmt.Fprintln(os.Stderr, "quorum:", err)
		os.Exit(1)
	}
}

func run(id int, peerList, dataDir string, snapEvery uint64) error {
	addrs := strings.Split(peerList, ",")
	if peerList == "" || id < 0 || id >= len(addrs) {
		return errors.New("--peers must list every node and --id must index into it")
	}
	if dataDir == "" {
		return errors.New("--data is required: Raft must be able to persist its state")
	}
	peers := make(map[int]string, len(addrs))
	ids := make([]int, len(addrs))
	for i, a := range addrs {
		peers[i], ids[i] = "http://"+strings.TrimSpace(a), i
	}

	store, err := raft.NewFileStorage(dataDir)
	if err != nil {
		return err
	}
	node := raft.New(id, ids, transport.NewHTTP(peers, 500*time.Millisecond), store,
		raft.WithSeed(time.Now().UnixNano()),
		raft.WithObserver(func(ev raft.Event) {
			if ev.Kind == raft.EventRole && (ev.Role == raft.Leader || ev.Role == raft.Follower) {
				log.Printf("node %d is %s in term %d", ev.Node, ev.Role, ev.Term)
			}
		}))
	server := kv.NewServer(node, kv.WithSnapshotEvery(snapEvery))

	mux := http.NewServeMux()
	mux.Handle("/raft/", transport.NewHTTPHandler(node))
	mux.Handle("/kv/", kv.NewHTTPHandler(server))
	mux.Handle("/status", kv.NewHTTPHandler(server))
	httpSrv := &http.Server{
		Addr:              strings.TrimSpace(addrs[id]),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errc := make(chan error, 1)
	go func() { errc <- httpSrv.ListenAndServe() }()
	log.Printf("node %d of %d listening on %s, state in %s", id, len(addrs), httpSrv.Addr, dataDir)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	select {
	case err := <-errc:
		return err
	case <-sig:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	server.Stop()
	node.Stop()
	return nil
}
