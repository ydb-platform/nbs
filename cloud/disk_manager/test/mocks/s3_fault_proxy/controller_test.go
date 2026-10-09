package s3_fault_proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type controllerFixture struct {
	dataURL    string
	controlURL string
	controller *Controller
	writes     *atomic.Int32
	client     *http.Client
}

func newControllerFixture(t *testing.T) controllerFixture {
	t.Helper()
	writes := &atomic.Int32{}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
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
	data := httptest.NewServer(proxy)
	t.Cleanup(data.Close)
	controller := NewController(proxy)
	control := httptest.NewServer(controller)
	t.Cleanup(control.Close)
	// Release handlers before closing httptest servers, also on assertion failure.
	t.Cleanup(controller.Close)
	return controllerFixture{
		dataURL: data.URL, controlURL: control.URL,
		controller: controller, writes: writes,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (f controllerFixture) set(t *testing.T, mode string, ttl int) {
	t.Helper()
	data, err := json.Marshal(faultRequest{
		Mode: mode, Method: http.MethodPut, PathPrefix: "/backup/", TTLSeconds: ttl,
	})
	if err != nil {
		t.Fatal(err)
	}
	response, err := f.client.Post(f.controlURL+"/fault", "application/json", bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("set fault returned %d", response.StatusCode)
	}
}

func (f controllerFixture) reset(t *testing.T) {
	t.Helper()
	response, err := f.client.Post(f.controlURL+"/reset", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.StatusCode)
	}
}

func (f controllerFixture) status(t *testing.T) controllerStatus {
	t.Helper()
	response, err := f.client.Get(f.controlURL + "/status")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(response.StatusCode)
	}
	var status controllerStatus
	if err := json.NewDecoder(response.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	return status
}

func (f controllerFixture) put(path string) (int, error) {
	request, err := http.NewRequest(http.MethodPut, f.dataURL+path, strings.NewReader("fixture"))
	if err != nil {
		return 0, err
	}
	response, err := f.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode, nil
}

func (f controllerFixture) startSuccessfulPut() <-chan error {
	done := make(chan error, 1)
	go func() {
		status, err := f.put("/backup/chunk")
		if err == nil && status != http.StatusOK {
			err = fmt.Errorf("released PUT returned HTTP %d", status)
		}
		done <- err
	}()
	return done
}

func requireRequestHeld(t *testing.T, f controllerFixture, done <-chan error, expectedWrites int32) {
	t.Helper()
	// A single nonblocking select can race the client's response reader.
	// Observe the hold for a bounded interval, checking both sides of the proxy.
	observation := time.NewTimer(100 * time.Millisecond)
	defer observation.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if actual := f.writes.Load(); actual != expectedWrites {
			t.Fatalf("wrong upstream boundary while held: writes=%d expected=%d", actual, expectedWrites)
		}
		select {
		case err := <-done:
			t.Fatalf("request completed before the hold was released: %v", err)
		case <-observation.C:
			select {
			case err := <-done:
				t.Fatalf("request was not held for the observation interval: %v", err)
			default:
			}
			if actual := f.writes.Load(); actual != expectedWrites {
				t.Fatalf("upstream changed while held: writes=%d expected=%d", actual, expectedWrites)
			}
			return
		case <-ticker.C:
		}
	}
}

func waitForStatus(t *testing.T, f controllerFixture, predicate func(controllerStatus) bool) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if predicate(f.status(t)) {
			return
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatal("controller did not reach expected state")
		}
	}
}

func TestControllerFail503AndReset(t *testing.T) {
	f := newControllerFixture(t)
	f.set(t, "fail503", 30)
	if code, err := f.put("/backup/chunk"); err != nil || code != 503 || f.writes.Load() != 0 {
		t.Fatalf("fault failed: status=%d err=%v writes=%d", code, err, f.writes.Load())
	}
	if code, err := f.put("/primary/chunk"); err != nil || code != 200 || f.writes.Load() != 1 {
		t.Fatalf("primary path affected: status=%d err=%v writes=%d", code, err, f.writes.Load())
	}
	status := f.status(t)
	if !status.Active || status.Hits != 1 || status.UpstreamAccepted != 0 || status.Mode != "fail503" {
		t.Fatalf("unexpected fault status: %+v", status)
	}
	f.reset(t)
	if code, err := f.put("/backup/chunk"); err != nil || code != 200 || f.writes.Load() != 2 {
		t.Fatalf("reset failed: status=%d err=%v writes=%d", code, err, f.writes.Load())
	}
	if status := f.status(t); status.Active || status.Mode != "pass" || status.Hits != 1 {
		t.Fatalf("reset must retain last fault counters: %+v", status)
	}
}

func TestControllerRejectsUnsafeFault(t *testing.T) {
	f := newControllerFixture(t)
	for _, body := range []string{
		`{"mode":"fail503","method":"PUT","path_prefix":"/backup/","ttl_seconds":0}`,
		`{"mode":"fail503","method":"PUT","path_prefix":"/backup/","ttl_seconds":301}`,
		`{"mode":"fail503","method":"GET","path_prefix":"/backup/","ttl_seconds":1}`,
		`{"mode":"fail503","method":"PUT","path_prefix":"/","ttl_seconds":1}`,
		`{"mode":"unknown","method":"PUT","path_prefix":"/backup/","ttl_seconds":1}`,
		`{"mode":"fail503","method":"PUT","path_prefix":"/backup/","ttl_seconds":1,"unknown":1}`,
		`{"mode":"fail503","method":"PUT","path_prefix":"/backup/","ttl_seconds":1} {}`,
		strings.Repeat("x", 5000),
	} {
		response, err := f.client.Post(f.controlURL+"/fault", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("unsafe configuration accepted: status=%d body=%q", response.StatusCode, body)
		}
	}
	if f.status(t).Active {
		t.Fatal("rejected configuration enabled a fault")
	}
}

func TestControllerExpiryReleasesHeldRequest(t *testing.T) {
	f := newControllerFixture(t)
	f.set(t, "hold-before-write", 1)
	done := f.startSuccessfulPut()
	waitForStatus(t, f, func(status controllerStatus) bool { return status.Hits == 1 })
	requireRequestHeld(t, f, done, 0)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("TTL did not release an already held request")
	}
	waitForStatus(t, f, func(status controllerStatus) bool { return !status.Active })
	if f.writes.Load() != 1 {
		t.Fatal("released request did not reach upstream")
	}
}

func TestControllerResetAfterAcceptedWrite(t *testing.T) {
	f := newControllerFixture(t)
	f.set(t, "hold-after-success", 30)
	done := f.startSuccessfulPut()
	waitForStatus(t, f, func(status controllerStatus) bool {
		return status.UpstreamAccepted == 1 && status.HeldAfterSuccess == 1
	})
	if f.writes.Load() != 1 {
		t.Fatal("upstream accepted counter precedes actual write")
	}
	requireRequestHeld(t, f, done, 1)
	f.reset(t)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reset did not release the response")
	}
	waitForStatus(t, f, func(status controllerStatus) bool {
		return status.UpstreamAccepted == 1 && status.HeldAfterSuccess == 0
	})
}

func TestControllerCancelledRequestIsNotCountedAsHeld(t *testing.T) {
	f := newControllerFixture(t)
	f.set(t, "hold-after-success", 30)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, f.dataURL+"/backup/chunk", strings.NewReader("fixture"))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		response, err := f.client.Do(request)
		if response != nil {
			response.Body.Close()
		}
		done <- err
	}()
	waitForStatus(t, f, func(status controllerStatus) bool {
		return status.UpstreamAccepted == 1 && status.HeldAfterSuccess == 1
	})
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled request must not receive a successful response")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancelled request did not complete")
	}
	waitForStatus(t, f, func(status controllerStatus) bool {
		return status.Active && status.UpstreamAccepted == 1 && status.HeldAfterSuccess == 0
	})
}

func TestControllerReplacementDoesNotCarryOldHeldCounter(t *testing.T) {
	f := newControllerFixture(t)
	f.set(t, "hold-after-success", 30)
	done := f.startSuccessfulPut()
	waitForStatus(t, f, func(status controllerStatus) bool { return status.HeldAfterSuccess == 1 })
	f.set(t, "hold-after-success", 30)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("replacement did not release old request")
	}
	status := f.status(t)
	if status.HeldAfterSuccess != 0 || status.UpstreamAccepted != 0 {
		t.Fatalf("old request corrupted new generation counters: %+v", status)
	}
	done = f.startSuccessfulPut()
	waitForStatus(t, f, func(status controllerStatus) bool {
		return status.UpstreamAccepted == 1 && status.HeldAfterSuccess == 1
	})
	f.reset(t)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("replacement request did not finish")
	}
	waitForStatus(t, f, func(status controllerStatus) bool { return status.HeldAfterSuccess == 0 })
}

func TestControllerDropBeforeAndAfterWrite(t *testing.T) {
	for _, mode := range []string{"drop-before-write", "drop-after-success"} {
		t.Run(mode, func(t *testing.T) {
			f := newControllerFixture(t)
			f.set(t, mode, 30)
			if _, err := f.put("/backup/chunk"); err == nil {
				t.Fatal("response must be lost")
			}
			want := int32(0)
			if mode == "drop-after-success" {
				want = 1
			}
			if f.writes.Load() != want || f.status(t).UpstreamAccepted != int(want) {
				t.Fatalf("wrong failure boundary: writes=%d expected=%d", f.writes.Load(), want)
			}
		})
	}
}

func TestControllerCloseReleasesHeldRequest(t *testing.T) {
	f := newControllerFixture(t)
	f.set(t, "hold-before-write", 30)
	done := f.startSuccessfulPut()
	waitForStatus(t, f, func(status controllerStatus) bool { return status.Hits == 1 })
	requireRequestHeld(t, f, done, 0)
	f.controller.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("controller shutdown left a held request")
	}
}
