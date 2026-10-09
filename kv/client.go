package kv

import (
	"context"
	"sync"
	"time"
)

// Endpoint is anything that can serve a Request: a *Server, or an adapter that
// reaches one over a network or looks it up again after a restart.
type Endpoint interface {
	Do(ctx context.Context, req Request) (Response, error)
}

// EndpointFunc adapts a function to Endpoint.
type EndpointFunc func(ctx context.Context, req Request) (Response, error)

func (f EndpointFunc) Do(ctx context.Context, req Request) (Response, error) { return f(ctx, req) }

// Clerk is a client. It talks to the replicas in turn until one accepts, and
// retries with the same sequence number on any failure, which the servers'
// dedup makes safe. A Clerk runs one operation at a time.
type Clerk struct {
	servers []Endpoint
	id      int64
	attempt time.Duration

	mu     sync.Mutex
	seq    int64
	leader int
}

// NewClerk creates a client with a caller-chosen unique id.
func NewClerk(id int64, servers []Endpoint) *Clerk {
	return &Clerk{servers: servers, id: id, attempt: 500 * time.Millisecond}
}

// ID is the client id the servers see.
func (c *Clerk) ID() int64 { return c.id }

func (c *Clerk) Get(ctx context.Context, key string) (string, error) {
	r, err := c.do(ctx, OpGet, key, "")
	return r.Value, err
}

func (c *Clerk) Put(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, OpPut, key, value)
	return err
}

func (c *Clerk) Append(ctx context.Context, key, value string) error {
	_, err := c.do(ctx, OpAppend, key, value)
	return err
}

func (c *Clerk) do(ctx context.Context, t OpType, key, value string) (Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seq++
	req := Request{Type: t, Key: key, Value: value, ClientID: c.id, Seq: c.seq}
	for fails := 0; ; {
		if err := ctx.Err(); err != nil {
			return Response{}, err
		}
		actx, cancel := context.WithTimeout(ctx, c.attempt)
		resp, err := c.servers[c.leader].Do(actx, req)
		cancel()
		if err == nil {
			return resp, nil
		}
		c.leader = (c.leader + 1) % len(c.servers)
		if fails++; fails%len(c.servers) == 0 {
			// Every replica refused in a row: an election is probably under
			// way, so back off briefly instead of spinning.
			select {
			case <-ctx.Done():
			case <-time.After(15 * time.Millisecond):
			}
		}
	}
}

// SetAttemptTimeout bounds each try against a single replica before the clerk
// moves on to the next one. The default is 500 ms.
func (c *Clerk) SetAttemptTimeout(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempt = d
}

// SetLeaderHint tells the clerk which replica to try first. Clerks remember
// the replica that last answered, so this only matters for the first request.
func (c *Clerk) SetLeaderHint(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.leader = i % len(c.servers)
}
