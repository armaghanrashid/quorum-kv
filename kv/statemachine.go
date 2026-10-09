// Package kv is a linearizable key-value store replicated with Raft. Clients
// send Get, Put and Append through a Clerk; the leader orders every operation,
// reads included, in the Raft log, and each server applies the log to an
// in-memory map. Per-client sequence numbers make retries idempotent.
package kv

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

// OpType is the kind of operation.
type OpType uint8

const (
	OpGet OpType = iota + 1
	OpPut
	OpAppend
)

func (t OpType) String() string {
	switch t {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	}
	return fmt.Sprintf("OpType(%d)", uint8(t))
}

// Request is one client operation. (ClientID, Seq) identifies it: a client
// numbers its operations 1, 2, 3... and has at most one outstanding, so the
// server needs to remember only the latest sequence number per client.
type Request struct {
	Type     OpType
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

// Response carries the value for a Get and nothing for writes.
type Response struct {
	Value string
}

// session is the dedup record for one client: the highest sequence number
// applied and, for a Get, the value it returned, so a retried Get gets the
// original answer instead of a newer one.
type session struct {
	Seq   int64
	Value string
}

// stateMachine is the deterministic part: given the same sequence of requests
// every replica ends in the same state.
type stateMachine struct {
	Data     map[string]string
	Sessions map[int64]session
}

func newStateMachine() *stateMachine {
	return &stateMachine{Data: make(map[string]string), Sessions: make(map[int64]session)}
}

// apply executes req at most once per (client, seq). A request older than the
// client's latest is a stale duplicate of something it already moved past;
// its result is irrelevant because the client no longer waits for it.
func (sm *stateMachine) apply(req Request) (resp Response, executed bool) {
	s := sm.Sessions[req.ClientID]
	if req.Seq <= s.Seq {
		if req.Seq == s.Seq {
			return Response{Value: s.Value}, false
		}
		return Response{}, false
	}
	switch req.Type {
	case OpGet:
		resp.Value = sm.Data[req.Key]
	case OpPut:
		sm.Data[req.Key] = req.Value
	case OpAppend:
		sm.Data[req.Key] += req.Value
	}
	sm.Sessions[req.ClientID] = session{Seq: req.Seq, Value: resp.Value}
	return resp, true
}

func (sm *stateMachine) snapshot() []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(sm); err != nil {
		panic("kv: encoding snapshot: " + err.Error())
	}
	return buf.Bytes()
}

func restoreStateMachine(data []byte) (*stateMachine, error) {
	sm := newStateMachine()
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(sm); err != nil {
		return nil, err
	}
	if sm.Data == nil {
		sm.Data = make(map[string]string)
	}
	if sm.Sessions == nil {
		sm.Sessions = make(map[int64]session)
	}
	return sm, nil
}

func encodeRequest(r Request) []byte {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(r); err != nil {
		panic("kv: encoding request: " + err.Error())
	}
	return buf.Bytes()
}

func decodeRequest(b []byte) (Request, error) {
	var r Request
	err := gob.NewDecoder(bytes.NewReader(b)).Decode(&r)
	return r, err
}
