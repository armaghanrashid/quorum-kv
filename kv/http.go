package kv

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"
)

// NewHTTPHandler serves the store over HTTP:
//
//	GET  /kv/{key}   -> the value (empty if unset)
//	PUT  /kv/{key}   -> sets the value to the request body
//	POST /kv/{key}   -> appends the request body to the value
//	GET  /status     -> the node's Raft status as JSON
//
// Writes and reads must reach the leader; any other node answers 503 with an
// X-Leader-Hint header holding the id of the node it believes leads. A client
// that wants exactly-once retries sends the same X-Client-ID and an
// increasing X-Seq; without them each request is treated as a fresh client.
func NewHTTPHandler(s *Server) http.Handler {
	mux := http.NewServeMux()
	serve := func(t OpType) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			req := Request{Type: t, Key: r.PathValue("key"), Value: string(body), ClientID: rand.Int63(), Seq: 1}
			if v := r.Header.Get("X-Client-ID"); v != "" {
				id, err := strconv.ParseInt(v, 10, 64)
				seq, err2 := strconv.ParseInt(r.Header.Get("X-Seq"), 10, 64)
				if err != nil || err2 != nil || seq < 1 {
					http.Error(w, "X-Client-ID and X-Seq must be integers, seq >= 1", http.StatusBadRequest)
					return
				}
				req.ClientID, req.Seq = id, seq
			}
			ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
			defer cancel()
			resp, err := s.Do(ctx, req)
			switch {
			case err == nil:
				io.WriteString(w, resp.Value)
			case errors.Is(err, ErrNotLeader):
				w.Header().Set("X-Leader-Hint", strconv.Itoa(s.node.Status().Leader))
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
			case errors.Is(err, context.DeadlineExceeded):
				http.Error(w, "timed out waiting for commit", http.StatusGatewayTimeout)
			default:
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		}
	}
	mux.HandleFunc("GET /kv/{key}", serve(OpGet))
	mux.HandleFunc("PUT /kv/{key}", serve(OpPut))
	mux.HandleFunc("POST /kv/{key}", serve(OpAppend))
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		st := s.node.Status()
		json.NewEncoder(w).Encode(map[string]any{
			"id": st.ID, "term": st.Term, "role": st.Role.String(), "leader": st.Leader,
			"commit_index": st.CommitIndex, "last_applied": st.LastApplied,
			"last_index": st.LastIndex, "snapshot_index": st.SnapshotIndex,
		})
	})
	return mux
}
