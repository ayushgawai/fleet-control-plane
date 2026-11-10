package fleet

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type Metrics struct{ Requests, Heartbeats, ConfigPushes, Conflicts, RateLimited atomic.Int64 }

type API struct {
	S       *Store
	Bus     *Bus
	Q       *Queue
	M       *Metrics
	RateLim int // per-key per minute; 0 = unlimited-ish default
}

func NewAPI() *API {
	return &API{S: NewStore(), Bus: NewBus(), Q: NewQueue(), M: &Metrics{}, RateLim: 6000}
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /devices", a.register)
	mux.HandleFunc("GET /devices", a.list)
	mux.HandleFunc("GET /devices/{id}", a.get)
	mux.HandleFunc("GET /devices/{id}/shadow", a.shadow)
	mux.HandleFunc("PUT /devices/{id}/desired", a.setDesired)
	mux.HandleFunc("POST /devices/{id}/heartbeat", a.heartbeat)
	mux.HandleFunc("POST /devices/{id}/ack", a.ack)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /metrics", a.metrics)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.M.Requests.Add(1)
		mux.ServeHTTP(w, r)
	})
}

func write(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
func errw(w http.ResponseWriter, code int, e error) {
	write(w, code, map[string]string{"error": e.Error()})
}

func (a *API) limited(w http.ResponseWriter, keys ...string) bool {
	for _, key := range keys {
		if !a.S.Allow(key, a.RateLim, time.Minute) {
			a.M.RateLimited.Add(1)
			errw(w, 429, ErrRate)
			return true
		}
	}
	return false
}

// device loads the path device and applies per-device + per-org rate limits.
func (a *API) device(w http.ResponseWriter, r *http.Request) (Device, bool) {
	d, err := a.S.Device(r.PathValue("id"))
	if err != nil {
		errw(w, 404, err)
		return d, false
	}
	return d, !a.limited(w, "device:"+d.ID, "org:"+d.Org)
}

func (a *API) register(w http.ResponseWriter, r *http.Request) {
	var d Device
	if json.NewDecoder(r.Body).Decode(&d) != nil || d.ID == "" || d.Org == "" {
		errw(w, 400, fmt.Errorf("id and org required"))
		return
	}
	if a.limited(w, "org:"+d.Org) {
		return
	}
	write(w, 201, a.S.Register(d))
}
func (a *API) list(w http.ResponseWriter, r *http.Request) {
	write(w, 200, a.S.List(r.URL.Query().Get("org")))
}
func (a *API) get(w http.ResponseWriter, r *http.Request) {
	d, err := a.S.Device(r.PathValue("id"))
	if err != nil {
		errw(w, 404, err)
		return
	}
	write(w, 200, map[string]any{"device": d, "online": a.S.Online(d.ID)})
}
func (a *API) shadow(w http.ResponseWriter, r *http.Request) {
	sh, err := a.S.Shadow(r.PathValue("id"))
	if err != nil {
		errw(w, 404, err)
		return
	}
	write(w, 200, sh)
}

// PUT desired config with optimistic concurrency: body {"config":{...},"expected_version":N}
// On success fans a Command out through the queue (SQS in prod).
func (a *API) setDesired(w http.ResponseWriter, r *http.Request) {
	d, ok := a.device(w, r)
	if !ok {
		return
	}
	id := d.ID
	var body struct {
		Config          map[string]any `json:"config"`
		ExpectedVersion int64          `json:"expected_version"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		errw(w, 400, fmt.Errorf("bad json"))
		return
	}
	sh, err := a.S.SetDesired(id, body.Config, body.ExpectedVersion)
	if err == ErrConflict {
		a.M.Conflicts.Add(1)
		errw(w, 409, err)
		return
	}
	if err != nil {
		errw(w, 404, err)
		return
	}
	a.Q.Send(Command{ID: fmt.Sprintf("%s:v%d", id, sh.Version), DeviceID: id, Version: sh.Version, Config: body.Config, SentAt: time.Now()})
	a.Bus.Publish("config-changes", []byte(fmt.Sprintf(`{"device":%q,"version":%d}`, id, sh.Version)))
	a.M.ConfigPushes.Add(1)
	write(w, 200, sh)
}

func (a *API) heartbeat(w http.ResponseWriter, r *http.Request) {
	d, ok := a.device(w, r)
	if !ok {
		return
	}
	a.S.Heartbeat(d.ID, 90*time.Second) // Redis SETEX presence in prod
	a.Bus.Publish("heartbeats", []byte(d.ID))
	a.M.Heartbeats.Add(1)
	write(w, 200, map[string]bool{"online": true})
}

// POST /devices/{id}/ack {"command_id":"...","version":N,"config":{...}}
// Idempotent: duplicate acks keep the highest reported version.
func (a *API) ack(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body struct {
		CommandID string         `json:"command_id"`
		Version   int64          `json:"version"`
		Config    map[string]any `json:"config"`
	}
	if json.NewDecoder(r.Body).Decode(&body) != nil {
		errw(w, 400, fmt.Errorf("bad json"))
		return
	}
	sh, err := a.S.Report(id, body.Config, body.Version)
	if err != nil {
		errw(w, 404, err)
		return
	}
	a.Q.Ack(body.CommandID)
	write(w, 200, sh)
}

// Prometheus text exposition format, hand-written to stay dependency-free.
func (a *API) metrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	for _, m := range []struct {
		name, typ string
		v         int64
	}{
		{"fleet_requests_total", "counter", a.M.Requests.Load()},
		{"fleet_heartbeats_total", "counter", a.M.Heartbeats.Load()},
		{"fleet_config_pushes_total", "counter", a.M.ConfigPushes.Load()},
		{"fleet_config_conflicts_total", "counter", a.M.Conflicts.Load()},
		{"fleet_rate_limited_total", "counter", a.M.RateLimited.Load()},
		{"fleet_queue_depth", "gauge", int64(a.Q.Len())},
		{"fleet_dlq_depth", "gauge", int64(a.Q.DLQLen())},
	} {
		fmt.Fprintf(w, "# TYPE %s %s\n%s %d\n", m.name, m.typ, m.name, m.v)
	}
}
