// Package fleet is the in-memory implementation of the interfaces that the
// production adapters (Postgres/DynamoDB/Redis/Kafka/SQS, see docker-compose.yml
// and migrations/) implement. One implementation is enough to test the logic;
// ponytail: global mutex, shard by org/device if throughput matters.
package fleet

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("version conflict") // optimistic-concurrency failure
	ErrRate     = errors.New("rate limited")
)

type Device struct {
	ID, Org, Model string
	CreatedAt      time.Time
}

type Shadow struct {
	DeviceID string
	Desired  map[string]any
	Reported map[string]any
	Version  int64 // bumped on every desired write; conditional writes check it

	// ReportedVersion is the desired version the device last applied.
	ReportedVersion int64
}

type Command struct {
	ID, DeviceID string
	Version      int64
	Config       map[string]any
	Attempts     int
	SentAt       time.Time
}

// Store: device registry + config shadows + presence + rate limits.
// In prod: registry/history -> Postgres, shadows -> DynamoDB conditional
// writes (ConditionExpression version = :expected), presence -> Redis SETEX,
// rate limits -> Redis token bucket. Same semantics, tested here.
type Store struct {
	mu       sync.Mutex
	devices  map[string]Device
	shadows  map[string]*Shadow
	presence map[string]time.Time // deviceID -> expiry
	rate     map[string][]time.Time
	History  []Shadow // append-only config history (Postgres in prod)
}

func NewStore() *Store {
	return &Store{devices: map[string]Device{}, shadows: map[string]*Shadow{}, presence: map[string]time.Time{}, rate: map[string][]time.Time{}}
}

func (s *Store) Register(d Device) Device {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d.CreatedAt.IsZero() {
		d.CreatedAt = time.Now()
	}
	s.devices[d.ID] = d
	if s.shadows[d.ID] == nil {
		s.shadows[d.ID] = &Shadow{DeviceID: d.ID, Desired: map[string]any{}, Reported: map[string]any{}}
	}
	return d
}

func (s *Store) Device(id string) (Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.devices[id]
	if !ok {
		return d, ErrNotFound
	}
	return d, nil
}

func (s *Store) List(org string) []Device {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Device{}
	for _, d := range s.devices {
		if org == "" || d.Org == org {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SetDesired is the DynamoDB conditional write: succeeds only if expected
// matches the stored version, then bumps it.
func (s *Store) SetDesired(id string, cfg map[string]any, expected int64) (Shadow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shadows[id]
	if !ok {
		return Shadow{}, ErrNotFound
	}
	if sh.Version != expected {
		return Shadow{}, ErrConflict
	}
	sh.Desired = cfg
	sh.Version++
	s.History = append(s.History, *sh)
	return *sh, nil
}

// Report is a device acknowledging/applying a config version. Idempotent:
// reporting a version <= current reported version is a no-op success, so
// duplicate SQS deliveries and retries never regress a device.
func (s *Store) Report(id string, cfg map[string]any, version int64) (Shadow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shadows[id]
	if !ok {
		return Shadow{}, ErrNotFound
	}
	if version <= sh.ReportedVersion {
		return *sh, nil
	}
	cp := make(map[string]any, len(cfg))
	for k, v := range cfg {
		cp[k] = v
	}
	sh.Reported, sh.ReportedVersion = cp, version
	return *sh, nil
}

func (s *Store) Shadow(id string) (Shadow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sh, ok := s.shadows[id]
	if !ok {
		return Shadow{}, ErrNotFound
	}
	return *sh, nil
}

func (s *Store) Heartbeat(id string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.presence[id] = time.Now().Add(ttl)
}

func (s *Store) Online(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.presence[id]
	return ok && time.Now().Before(exp)
}

// Allow: sliding-window rate limit per key (per-device / per-org in prod Redis).
func (s *Store) Allow(key string, limit int, window time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	q := s.rate[key][:0]
	for _, t := range s.rate[key] {
		if now.Sub(t) < window {
			q = append(q, t)
		}
	}
	if len(q) >= limit {
		s.rate[key] = q
		return false
	}
	s.rate[key] = append(q, now)
	return true
}

// Bus is the in-memory Kafka fake: append-only topics.
type Bus struct {
	mu     sync.Mutex
	Topics map[string][][]byte
}

func NewBus() *Bus { return &Bus{Topics: map[string][][]byte{}} }
func (b *Bus) Publish(topic string, msg []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.Topics[topic] = append(b.Topics[topic], msg)
}
func (b *Bus) Count(topic string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.Topics[topic])
}

// Queue is the in-memory SQS fake: explicit-Fail retries, DLQ after
// MaxAttempts, idempotent Ack (acking twice / acking a DLQ'd message is a
// no-op). ponytail: no visibility timeout, a consumer that crashes without
// Fail loses the message; real SQS redelivers after VisibilityTimeout.
type Queue struct {
	mu          sync.Mutex
	pending     []Command
	DLQ         []Command
	acked       map[string]bool
	MaxAttempts int
}

func NewQueue() *Queue { return &Queue{acked: map[string]bool{}, MaxAttempts: 3} }
func (q *Queue) Send(c Command) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.acked[c.ID] {
		return // duplicate send of an already-acked command
	}
	q.pending = append(q.pending, c)
}
func (q *Queue) Receive() (Command, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return Command{}, false
	}
	c := q.pending[0]
	q.pending = q.pending[1:]
	c.Attempts++
	return c, true
}
func (q *Queue) Ack(id string) { // idempotent
	q.mu.Lock()
	defer q.mu.Unlock()
	q.acked[id] = true
	for i, c := range q.pending {
		if c.ID == id {
			q.pending = append(q.pending[:i], q.pending[i+1:]...)
			break
		}
	}
}
func (q *Queue) Fail(c Command) { // retry or dead-letter
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.acked[c.ID] {
		return
	}
	if c.Attempts >= q.MaxAttempts {
		q.DLQ = append(q.DLQ, c)
		return
	}
	q.pending = append(q.pending, c)
}
func (q *Queue) Acked(id string) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.acked[id]
}
func (q *Queue) Len() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.pending) }
func (q *Queue) DLQLen() int { q.mu.Lock(); defer q.mu.Unlock(); return len(q.DLQ) }
