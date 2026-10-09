// Package s3_fault_proxy injects faults into a local S3 emulator in tests.
// It is not a proxy for real buckets or a production service.
package s3_fault_proxy

import (
	"context"
	"encoding/xml"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Fault applies to matching requests until Clear is called. Set disables any
// previous fault. Configure faults before starting requests; Clear does not
// change requests already in flight. Close WaitFor to release a blocked request.
type Fault struct {
	Method            string
	PathPrefix        string
	StatusCode        int
	ErrorCode         string
	DropResponse      bool
	DropBeforeWrite   bool
	WaitFor           <-chan struct{}
	WaitAfterResponse <-chan struct{}
	Reached           chan<- struct{}
	generation        uint64
}

type faultContextKey struct{}

type Proxy struct {
	proxy            *httputil.ReverseProxy
	mu               sync.Mutex
	fault            *Fault
	hits             int
	accepted         int
	heldAfterSuccess int
	generation       uint64
}

func New(endpoint string) (*Proxy, error) {
	upstream, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(upstream.Hostname())
	if upstream.Scheme != "http" || upstream.User != nil ||
		(upstream.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
		return nil, fmt.Errorf("fault proxy requires a loopback HTTP S3 emulator")
	}

	p := &Proxy{proxy: httputil.NewSingleHostReverseProxy(upstream)}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.MaxConnsPerHost = 64
	p.proxy.Transport = transport
	p.proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		http.Error(w, "local test upstream failed", http.StatusBadGateway)
	}
	p.proxy.ModifyResponse = func(response *http.Response) error {
		fault, ok := response.Request.Context().Value(faultContextKey{}).(Fault)
		if !ok || response.StatusCode >= 300 {
			return nil
		}
		p.mu.Lock()
		if p.generation == fault.generation {
			p.accepted++
			if fault.WaitAfterResponse != nil {
				p.heldAfterSuccess++
			}
		}
		p.mu.Unlock()
		if fault.WaitAfterResponse != nil {
			defer func() {
				p.mu.Lock()
				defer p.mu.Unlock()
				if p.generation == fault.generation {
					p.heldAfterSuccess--
				}
			}()
			select {
			case <-fault.WaitAfterResponse:
			case <-response.Request.Context().Done():
				return response.Request.Context().Err()
			}
		}
		if fault.DropResponse {
			// The upstream has accepted the write, but the client must not see
			// a success response. Abort rather than synthesize another status.
			response.Body.Close()
			panic(http.ErrAbortHandler)
		}
		return nil
	}
	return p, nil
}

func (p *Proxy) Set(fault Fault) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.generation++
	fault.generation = p.generation
	p.fault = &fault
	p.hits = 0
	p.accepted = 0
	p.heldAfterSuccess = 0
}

func (p *Proxy) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fault = nil
}

func (p *Proxy) Hits() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits
}

func (p *Proxy) Stats() (int, int) {
	hits, accepted, _ := p.statusCounters()
	return hits, accepted
}

func (p *Proxy) statusCounters() (int, int, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.hits, p.accepted, p.heldAfterSuccess
}

func (p *Proxy) Close() {
	if transport, ok := p.proxy.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

func (p *Proxy) match(request *http.Request) (Fault, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.fault == nil {
		return Fault{}, false
	}
	fault := *p.fault
	if fault.Method != "" && fault.Method != request.Method {
		return Fault{}, false
	}
	if !strings.HasPrefix(request.URL.Path, fault.PathPrefix) {
		return Fault{}, false
	}
	p.hits++
	return fault, true
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	fault, matched := p.match(request)
	if matched {
		if fault.Reached != nil {
			select {
			case fault.Reached <- struct{}{}:
			default:
			}
		}
		if fault.WaitFor != nil {
			select {
			case <-fault.WaitFor:
			case <-request.Context().Done():
				return
			}
		}
		if fault.DropBeforeWrite {
			panic(http.ErrAbortHandler)
		}
		if fault.StatusCode != 0 {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(fault.StatusCode)
			code := "ServiceUnavailable"
			if fault.StatusCode == http.StatusForbidden {
				code = "AccessDenied"
			} else if fault.StatusCode == http.StatusTooManyRequests {
				code = "SlowDown"
			}
			if fault.ErrorCode != "" {
				code = fault.ErrorCode
			}
			_ = xml.NewEncoder(w).Encode(struct {
				XMLName xml.Name `xml:"Error"`
				Code    string   `xml:"Code"`
				Message string   `xml:"Message"`
			}{Code: code, Message: "injected test fault"})
			return
		}
		request = request.WithContext(context.WithValue(request.Context(), faultContextKey{}, fault))
	}
	p.proxy.ServeHTTP(w, request)
}
