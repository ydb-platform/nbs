package s3_fault_proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

const maxFaultTTL = 300 * time.Second

// Controller is a loopback recipe control plane, not a service for real S3.
// Reset, expiry and Close release requests held by the current fault.
type Controller struct {
	mu         sync.Mutex
	proxy      *Proxy
	mode       string
	expiresAt  time.Time
	timer      *time.Timer
	release    chan struct{}
	generation uint64
	closed     bool
}

type faultRequest struct {
	Mode       string `json:"mode"`
	Method     string `json:"method"`
	PathPrefix string `json:"path_prefix"`
	TTLSeconds int    `json:"ttl_seconds"`
}

type controllerStatus struct {
	Active           bool      `json:"active"`
	Mode             string    `json:"mode"`
	Hits             int       `json:"hits"`
	UpstreamAccepted int       `json:"upstream_accepted"`
	HeldAfterSuccess int       `json:"held_after_success"`
	ExpiresAt        time.Time `json:"expires_at"`
}

func NewController(proxy *Proxy) *Controller {
	return &Controller{proxy: proxy, mode: "pass"}
}

func (c *Controller) resetLocked() {
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	if c.release != nil {
		close(c.release)
		c.release = nil
	}
	c.proxy.Clear()
	c.mode = "pass"
	c.expiresAt = time.Time{}
	c.generation++
}

func (c *Controller) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	c.resetLocked()
}

func (c *Controller) statusLocked() controllerStatus {
	hits, accepted, held := c.proxy.statusCounters()
	return controllerStatus{
		Active: c.mode != "pass", Mode: c.mode, Hits: hits,
		UpstreamAccepted: accepted, HeldAfterSuccess: held, ExpiresAt: c.expiresAt,
	}
}

func writeStatus(w http.ResponseWriter, status controllerStatus) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(status)
}

func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/status" && r.Method == http.MethodGet {
		c.mu.Lock()
		status := c.statusLocked()
		c.mu.Unlock()
		writeStatus(w, status)
		return
	}
	if (r.URL.Path == "/reset" && r.Method == http.MethodPost) ||
		(r.URL.Path == "/fault" && r.Method == http.MethodDelete) {
		c.mu.Lock()
		c.resetLocked()
		status := c.statusLocked()
		c.mu.Unlock()
		writeStatus(w, status)
		return
	}
	if r.URL.Path != "/fault" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	decoder.DisallowUnknownFields()
	var request faultRequest
	if decoder.Decode(&request) != nil || decoder.Decode(new(interface{})) != io.EOF {
		http.Error(w, "one JSON fault configuration is required", http.StatusBadRequest)
		return
	}
	if request.TTLSeconds < 1 || request.TTLSeconds > int(maxFaultTTL/time.Second) {
		http.Error(w, "ttl_seconds must be between 1 and 300", http.StatusBadRequest)
		return
	}
	// Faults cannot accidentally affect reads or an entire upstream: select
	// PUT and an explicit bucket/object path prefix.
	if request.Method != http.MethodPut || !strings.HasPrefix(request.PathPrefix, "/") ||
		request.PathPrefix == "/" || len(request.PathPrefix) > 1024 {
		http.Error(w, "fault requires method PUT and a non-root path_prefix", http.StatusBadRequest)
		return
	}
	fault := Fault{Method: request.Method, PathPrefix: request.PathPrefix}
	var release chan struct{}
	switch request.Mode {
	case "fail503":
		fault.StatusCode = http.StatusServiceUnavailable
	case "drop-before-write":
		fault.DropBeforeWrite = true
	case "drop-after-success":
		fault.DropResponse = true
	case "hold-before-write":
		release = make(chan struct{})
		fault.WaitFor = release
	case "hold-after-success":
		release = make(chan struct{})
		fault.WaitAfterResponse = release
	default:
		http.Error(w, "unknown fault mode", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		http.Error(w, "controller is closed", http.StatusServiceUnavailable)
		return
	}
	c.resetLocked()
	c.release = release
	c.mode = request.Mode
	c.expiresAt = time.Now().Add(time.Duration(request.TTLSeconds) * time.Second)
	c.proxy.Set(fault)
	generation := c.generation
	c.timer = time.AfterFunc(time.Until(c.expiresAt), func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.generation == generation {
			c.resetLocked()
		}
	})
	status := c.statusLocked()
	c.mu.Unlock()
	writeStatus(w, status)
}
