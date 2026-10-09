package transport

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/armaghanrashid/quorum-kv/raft"
)

// HTTP is a raft.Transport that POSTs JSON to peers. peers maps node id to a
// base URL such as http://localhost:7001. Each call is bounded by a timeout so
// a dead peer costs one timeout, not a hung goroutine.
type HTTP struct {
	peers  map[int]string
	client *http.Client
}

// NewHTTP builds the client side. A zero timeout defaults to 500 ms.
func NewHTTP(peers map[int]string, timeout time.Duration) *HTTP {
	if timeout == 0 {
		timeout = 500 * time.Millisecond
	}
	return &HTTP{peers: peers, client: &http.Client{Timeout: timeout}}
}

func (h *HTTP) post(to int, path string, in, out any) error {
	base, ok := h.peers[to]
	if !ok {
		return fmt.Errorf("transport: unknown peer %d", to)
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	resp, err := h.client.Post(base+path, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("transport: %s returned %s", path, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (h *HTTP) RequestVote(to int, a *raft.RequestVoteArgs) (*raft.RequestVoteReply, error) {
	var r raft.RequestVoteReply
	if err := h.post(to, "/raft/request-vote", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (h *HTTP) AppendEntries(to int, a *raft.AppendEntriesArgs) (*raft.AppendEntriesReply, error) {
	var r raft.AppendEntriesReply
	if err := h.post(to, "/raft/append-entries", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (h *HTTP) InstallSnapshot(to int, a *raft.InstallSnapshotArgs) (*raft.InstallSnapshotReply, error) {
	var r raft.InstallSnapshotReply
	if err := h.post(to, "/raft/install-snapshot", a, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// NewHTTPHandler exposes a Handler (a *raft.Node) over the three RPC routes.
func NewHTTPHandler(h raft.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /raft/request-vote", rpc(func(a *raft.RequestVoteArgs) any { return h.HandleRequestVote(a) }))
	mux.HandleFunc("POST /raft/append-entries", rpc(func(a *raft.AppendEntriesArgs) any { return h.HandleAppendEntries(a) }))
	mux.HandleFunc("POST /raft/install-snapshot", rpc(func(a *raft.InstallSnapshotArgs) any { return h.HandleInstallSnapshot(a) }))
	return mux
}

func rpc[A any](f func(*A) any) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var a A
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<20)).Decode(&a); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(f(&a))
	}
}
