package s3_fault_proxy

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRejectRemoteUpstream(t *testing.T) {
	for _, endpoint := range []string{"https://storage.example.org", "http://192.0.2.1", "http://user:pass@localhost"} {
		if _, err := New(endpoint); err == nil {
			t.Fatalf("accepted non-test endpoint %q", endpoint)
		}
	}
}

func TestFaults(t *testing.T) {
	var writes atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.Method == http.MethodPut {
			writes.Add(1)
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(upstream.Close)
	proxy, err := New(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	client := &http.Client{Timeout: 5 * time.Second}
	put := func(path string) (int, error) {
		request, err := http.NewRequest(http.MethodPut, server.URL+path, bytes.NewBufferString("data"))
		if err != nil {
			return 0, err
		}
		response, err := client.Do(request)
		if err != nil {
			return 0, err
		}
		defer response.Body.Close()
		io.Copy(io.Discard, response.Body)
		return response.StatusCode, nil
	}

	proxy.Set(Fault{Method: http.MethodPut, PathPrefix: "/backup/", StatusCode: 503})
	if code, err := put("/backup/chunk"); err != nil || code != 503 || writes.Load() != 0 {
		t.Fatalf("failure before write: status=%d err=%v writes=%d", code, err, writes.Load())
	}
	if code, err := put("/primary/chunk"); err != nil || code != 200 || writes.Load() != 1 {
		t.Fatalf("unrelated path was affected: status=%d err=%v writes=%d", code, err, writes.Load())
	}
	response, err := client.Get(server.URL + "/backup/chunk")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 || proxy.Hits() != 1 {
		t.Fatal("method selection failed")
	}

	proxy.Set(Fault{Method: http.MethodPut, DropResponse: true})
	if _, err := put("/backup/chunk"); err == nil || writes.Load() != 2 || proxy.Hits() != 1 {
		t.Fatalf("lost response: err=%v writes=%d hits=%d", err, writes.Load(), proxy.Hits())
	}
	proxy.Clear()
	if code, err := put("/backup/chunk"); err != nil || code != 200 || writes.Load() != 3 {
		t.Fatalf("recovery failed: status=%d err=%v writes=%d", code, err, writes.Load())
	}
}

func TestBlockedRequestCanBeCancelled(t *testing.T) {
	var writes atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writes.Add(1)
	}))
	t.Cleanup(upstream.Close)
	proxy, err := New(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	gate := make(chan struct{})
	// LIFO: release the held request before server.Close drains handlers,
	// then close the proxy transport and finally the upstream server.
	t.Cleanup(func() { close(gate) })
	reached := make(chan struct{}, 1)
	proxy.Set(Fault{WaitFor: gate, Reached: reached})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		response, err := server.Client().Do(request)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("request did not reach the barrier")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil || writes.Load() != 0 {
			t.Fatalf("cancelled request reached upstream: err=%v writes=%d", err, writes.Load())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request did not stop after cancellation")
	}
}

func TestSlowDownDoesNotReachUpstream(t *testing.T) {
	var requests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
	}))
	t.Cleanup(upstream.Close)
	proxy, err := New(upstream.URL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proxy.Close)
	server := httptest.NewServer(proxy)
	t.Cleanup(server.Close)
	proxy.Set(Fault{StatusCode: http.StatusServiceUnavailable, ErrorCode: "SlowDown"})
	response, err := server.Client().Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body struct {
		Code string `xml:"Code"`
	}
	if err := xml.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || body.Code != "SlowDown" || requests.Load() != 0 {
		t.Fatalf("invalid throttle response: status=%d code=%q upstream=%d", response.StatusCode, body.Code, requests.Load())
	}
}
