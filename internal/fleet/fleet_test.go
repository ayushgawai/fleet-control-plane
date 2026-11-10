package fleet

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestOptimisticConcurrency(t *testing.T) {
	s := NewStore()
	s.Register(Device{ID: "cam1", Org: "o1"})
	if _, err := s.SetDesired("cam1", map[string]any{"fps": 15.0}, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := s.SetDesired("cam1", map[string]any{"fps": 30.0}, 0); err != ErrConflict {
		t.Fatalf("stale writer should conflict, got %v", err)
	}
	sh, _ := s.Shadow("cam1")
	if sh.Version != 1 || sh.Desired["fps"] != 15.0 {
		t.Fatalf("bad shadow %+v", sh)
	}
}

func TestPresenceTTLAndRateLimit(t *testing.T) {
	s := NewStore()
	s.Register(Device{ID: "cam1", Org: "o1"})
	s.Heartbeat("cam1", 30*time.Millisecond)
	if !s.Online("cam1") {
		t.Fatal("should be online")
	}
	time.Sleep(40 * time.Millisecond)
	if s.Online("cam1") {
		t.Fatal("presence should expire")
	}
	for i := 0; i < 3; i++ {
		if !s.Allow("device:cam1", 3, time.Minute) {
			t.Fatal("premature limit")
		}
	}
	if s.Allow("device:cam1", 3, time.Minute) {
		t.Fatal("4th request should be limited")
	}
}

func TestQueueRetryDLQAndIdempotentAck(t *testing.T) {
	q := NewQueue()
	q.Send(Command{ID: "c1", DeviceID: "cam1"})
	for i := 0; i < 3; i++ {
		c, ok := q.Receive()
		if !ok {
			t.Fatal("expected message")
		}
		q.Fail(c)
	}
	if len(q.DLQ) != 1 {
		t.Fatalf("expected DLQ after max attempts, got %d", len(q.DLQ))
	}
	q.Send(Command{ID: "c2", DeviceID: "cam1"})
	q.Send(Command{ID: "c2", DeviceID: "cam1"}) // duplicate delivery
	if !q.Ack("c2") || q.Ack("c2") {
		t.Fatal("first ack must report true, duplicate false")
	}
	if !q.Acked("c2") || q.Len() != 0 {
		t.Fatal("idempotent ack failed")
	}
	if _, ok := q.Receive(); ok {
		t.Fatal("acked duplicates must not be redelivered")
	}
}

// Duplicate/out-of-order deliveries must never regress reported config.
func TestReportIdempotentNoStale(t *testing.T) {
	s := NewStore()
	s.Register(Device{ID: "cam1", Org: "o1"})
	s.Report("cam1", map[string]any{"fps": 30.0}, 2)
	s.Report("cam1", map[string]any{"fps": 15.0}, 1) // stale duplicate
	s.Report("cam1", map[string]any{"fps": 30.0}, 2) // exact duplicate
	sh, _ := s.Shadow("cam1")
	if sh.Reported["fps"] != 30.0 || sh.ReportedVersion != 2 {
		t.Fatalf("device regressed to stale config: %+v", sh)
	}
	if _, err := s.Report("ghost", nil, 1); err != ErrNotFound {
		t.Fatalf("unknown device report should be ErrNotFound, got %v", err)
	}
}

func TestAPIRateLimitPerDevice(t *testing.T) {
	a := NewAPI()
	a.RateLim = 2
	a.S.Register(Device{ID: "cam1", Org: "o1"})
	codes := []int{}
	for i := 0; i < 3; i++ {
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, httptest.NewRequest("POST", "/devices/cam1/heartbeat", nil))
		codes = append(codes, w.Code)
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("want 200,200,429 got %v", codes)
	}
}

func TestAPIEndToEnd(t *testing.T) {
	a := NewAPI()
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	post := func(path, body string) *http.Response {
		r, err := http.Post(srv.URL+path, "application/json", bytes.NewBufferString(body))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	if r := post("/devices", `{"id":"cam1","org":"o1","model":"x"}`); r.StatusCode != 201 {
		t.Fatalf("register %d", r.StatusCode)
	}
	if r := post("/devices/cam1/heartbeat", `{}`); r.StatusCode != 200 {
		t.Fatalf("heartbeat %d", r.StatusCode)
	}
	req, _ := http.NewRequest("PUT", srv.URL+"/devices/cam1/desired", bytes.NewBufferString(`{"config":{"fps":30},"expected_version":0}`))
	r, err := http.DefaultClient.Do(req)
	if err != nil || r.StatusCode != 200 {
		t.Fatalf("desired %v", err)
	}
	var sh Shadow
	json.NewDecoder(r.Body).Decode(&sh)
	if sh.Version != 1 {
		t.Fatalf("version %d", sh.Version)
	}
	if r := post("/devices/cam1/ack", `{"command_id":"cam1:v1","version":1,"config":{"fps":30}}`); r.StatusCode != 200 {
		t.Fatalf("ack %d", r.StatusCode)
	}
	r2, _ := http.Get(srv.URL + "/devices/cam1/shadow")
	var sh2 Shadow
	json.NewDecoder(r2.Body).Decode(&sh2)
	if sh2.Reported["fps"] != 30.0 {
		t.Fatalf("reported %+v", sh2.Reported)
	}
	if r, _ := http.Get(srv.URL + "/metrics"); r.StatusCode != 200 {
		t.Fatal("metrics")
	}
}
