// A loopback-only fault injector for the backup acceptance tests.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	common "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type rule struct {
	ID             string `json:"id"`
	Route          string `json:"route"`
	Method         string `json:"method"`
	Contains       string `json:"contains"`
	DiskID         string `json:"disk_id"`
	Mode           string `json:"mode"`
	BytesPerSecond int64  `json:"bytes_per_second"`
}
type event struct {
	Sequence                          int `json:"sequence"`
	Route, Method, Key, Rule, Outcome string
	Started, Ended                    time.Time
	Bytes                             int
}
type activeRule struct {
	rule
	release chan struct{}
	next    time.Time
}
type injector struct {
	mu     sync.Mutex
	rules  []*activeRule
	events []event
	log    io.Writer
}

func (p *injector) record(e event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e.Sequence = len(p.events) + 1
	p.events = append(p.events, e)
	if err := json.NewEncoder(p.log).Encode(e); err != nil {
		log.Printf("event log: %v", err)
	}
}
func (p *injector) selectRule(route, method, key string) *activeRule {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, r := range p.rules {
		if r.Route == route && (r.Method == "" || r.Method == method) && strings.Contains(key, r.Contains) && (r.DiskID == "" || r.DiskID == key) {
			return r
		}
	}
	return nil
}
func (p *injector) wait(ctx context.Context, r *activeRule, n int) error {
	if r == nil {
		return nil
	}
	if r.Mode == "gate" {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.release:
			return nil
		}
	}
	if r.Mode != "rate" || n == 0 {
		return nil
	}
	p.mu.Lock()
	start := time.Now()
	if r.next.After(start) {
		start = r.next
	}
	r.next = start.Add(time.Duration(float64(n) / float64(r.BytesPerSecond) * float64(time.Second)))
	until := r.next
	p.mu.Unlock()
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.release:
		return nil
	case <-timer.C:
		return nil
	}
}
func (p *injector) control(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == "GET" {
		p.mu.Lock()
		defer p.mu.Unlock()
		_ = json.NewEncoder(w).Encode(p.events)
		return
	}
	if r.Method != "PUT" {
		http.Error(w, "GET or PUT", 405)
		return
	}
	var rules []rule
	if err := json.NewDecoder(io.LimitReader(r.Body, 65536)).Decode(&rules); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	seen := map[string]bool{}
	for _, r := range rules {
		if r.ID == "" || seen[r.ID] || (r.Route != "nbs" && r.Route != "primary" && r.Route != "backup") {
			http.Error(w, "invalid rule identity/route", 400)
			return
		}
		if r.Route == "nbs" && r.Mode == "permanent" && r.Method != "CreateCheckpoint" {
			http.Error(w, "permanent NBS error supports CreateCheckpoint only", 400)
			return
		}
		seen[r.ID] = true
		switch r.Mode {
		case "error", "permanent", "gate", "lost-reply":
		case "rate":
			if r.BytesPerSecond <= 0 {
				http.Error(w, "positive bandwidth required", 400)
				return
			}
		default:
			http.Error(w, "invalid mode", 400)
			return
		}
	}
	p.mu.Lock()
	for _, r := range p.rules {
		close(r.release)
	}
	p.rules = nil
	for _, r := range rules {
		p.rules = append(p.rules, &activeRule{rule: r, release: make(chan struct{})})
	}
	p.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]int{"rules": len(rules)})
}
func (p *injector) httpProxy(route, upstream string) http.Handler {
	transport := &http.Transport{MaxIdleConnsPerHost: 100}
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		e := event{Route: route, Method: request.Method, Key: request.URL.Path, Started: time.Now(), Outcome: "error"}
		defer func() { e.Ended = time.Now(); p.record(e) }()
		r := p.selectRule(route, request.Method, request.URL.Path)
		if r != nil {
			e.Rule = r.ID
			p.record(event{Route: route, Method: request.Method, Key: e.Key, Rule: r.ID, Started: e.Started, Outcome: "injection-enter"})
			if r.Mode == "error" || r.Mode == "permanent" {
				w.Header().Set("Content-Type", "application/xml")
				code := 503
				if r.Mode == "permanent" {
					code = 400
				}
				w.WriteHeader(code)
				_, _ = io.WriteString(w, "<Error><Code>ServiceUnavailable</Code><Message>backup test injection</Message></Error>")
				e.Outcome = "injected-error"
				return
			}
		}
		body, err := io.ReadAll(io.LimitReader(request.Body, (64<<20)+1))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		if len(body) > 64<<20 {
			http.Error(w, "request exceeds test limit", 413)
			return
		}
		if err = p.wait(request.Context(), r, len(body)); err != nil {
			http.Error(w, err.Error(), 504)
			return
		}
		out, err := http.NewRequestWithContext(request.Context(), request.Method, upstream+request.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		out.Header = request.Header.Clone()
		out.Host = request.Host
		response, err := transport.RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, (64<<20)+1))
		if err != nil {
			http.Error(w, err.Error(), 502)
			return
		}
		if len(data) > 64<<20 {
			http.Error(w, "response exceeds test limit", 502)
			return
		}
		if len(body) == 0 {
			if err = p.wait(request.Context(), r, len(data)); err != nil {
				http.Error(w, err.Error(), 504)
				return
			}
		}
		e.Bytes = len(body) + len(data)
		if r != nil && r.Mode == "lost-reply" {
			connection, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = connection.Close()
			}
			e.Outcome = "reply-lost-after-upstream"
			return
		}
		for key, values := range response.Header {
			for _, value := range values {
				w.Header().Add(key, value)
			}
		}
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(data)
		e.Outcome = fmt.Sprint(response.StatusCode)
	})
}

type rawCodec struct{}

func (rawCodec) Name() string                          { return "proto" }
func (rawCodec) Marshal(v interface{}) ([]byte, error) { return *v.(*[]byte), nil }
func (rawCodec) Unmarshal(b []byte, v interface{}) error {
	*v.(*[]byte) = append((*v.(*[]byte))[:0], b...)
	return nil
}
func (p *injector) grpcProxy(connection *grpc.ClientConn, port uint32) grpc.StreamHandler {
	return func(_ interface{}, stream grpc.ServerStream) error {
		method, _ := grpc.MethodFromServerStream(stream)
		short := method[strings.LastIndex(method, "/")+1:]
		var request, response []byte
		if err := stream.RecvMsg(&request); err != nil {
			return err
		}
		// DiskId is field 2 in NBS requests. The generated ReadBlocks message
		// preserves that field while ignoring unrelated fields for other methods.
		var identity nbs.TReadBlocksRequest
		_ = proto.Unmarshal(request, &identity)
		e := event{Route: "nbs", Method: short, Key: identity.DiskId, Started: time.Now(), Outcome: "error"}
		defer func() { e.Ended = time.Now(); p.record(e) }()
		r := p.selectRule("nbs", short, identity.DiskId)
		if r != nil {
			e.Rule = r.ID
			p.record(event{Route: "nbs", Method: short, Key: e.Key, Rule: r.ID, Started: e.Started, Outcome: "injection-enter"})
			if r.Mode == "error" {
				e.Outcome = "injected-unavailable"
				return status.Error(codes.Unavailable, "backup test injection")
			}
			if r.Mode == "permanent" {
				e.Outcome = "injected-nbs-argument"
				// Transport gRPC InvalidArgument is retryable in the NBS SDK.
				// Use the application's response error for irreversible failure.
				payload, err := proto.Marshal(&nbs.TCreateCheckpointResponse{
					Error: &common.TError{Code: nbsclient.E_ARGUMENT, Message: "backup test injection"},
				})
				if err != nil {
					return err
				}
				e.Bytes = len(payload)
				return stream.SendMsg(&payload)
			}
			if err := p.wait(stream.Context(), r, 0); err != nil {
				return status.FromContextError(err).Err()
			}
		}
		ctx := stream.Context()
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			ctx = metadata.NewOutgoingContext(ctx, md.Copy())
		}
		var headers, trailers metadata.MD
		if err := connection.Invoke(ctx, method, &request, &response, grpc.ForceCodec(rawCodec{}), grpc.Header(&headers), grpc.Trailer(&trailers)); err != nil {
			return err
		}
		if short == "DiscoverInstances" {
			var discovery nbs.TDiscoverInstancesResponse
			if err := proto.Unmarshal(response, &discovery); err != nil {
				return err
			}
			for _, instance := range discovery.Instances {
				instance.Host = "localhost"
				instance.Port = port
			}
			var err error
			response, err = proto.Marshal(&discovery)
			if err != nil {
				return err
			}
		}
		if err := p.wait(ctx, r, len(response)); err != nil {
			return status.FromContextError(err).Err()
		}
		e.Bytes = len(response)
		if r != nil && r.Mode == "lost-reply" {
			e.Outcome = "reply-lost-after-upstream"
			return status.Error(codes.Unavailable, "backup test lost reply")
		}
		if err := stream.SendHeader(headers); err != nil {
			return err
		}
		stream.SetTrailer(trailers)
		e.Outcome = "ok"
		return stream.SendMsg(&response)
	}
}
func main() {
	nbsUp := flag.String("nbs-upstream", "", "TLS NBS endpoint")
	certFile := flag.String("cert", "", "loopback server certificate")
	keyFile := flag.String("key", "", "loopback server key")
	grpcPort := flag.Int("grpc-port", 0, "TLS NBS proxy port")
	primaryUp := flag.String("primary-upstream", "", "loopback primary S3 URL")
	backupUp := flag.String("backup-upstream", "", "loopback backup S3 URL")
	primaryPort := flag.Int("primary-port", 0, "primary proxy port")
	backupPort := flag.Int("backup-port", 0, "backup proxy port")
	controlPort := flag.Int("control-port", 0, "control port")
	logfile := flag.String("events", "", "JSONL evidence")
	flag.Parse()
	f, err := os.OpenFile(*logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	p := &injector{log: f, events: []event{}}
	roots := x509.NewCertPool()
	pem, err := os.ReadFile(*certFile)
	if err != nil {
		log.Fatal(err)
	}
	if !roots.AppendCertsFromPEM(pem) {
		log.Fatal("no certificate")
	}
	connection, err := grpc.Dial(*nbsUp, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(64<<20), grpc.MaxCallSendMsgSize(64<<20)))
	if err != nil {
		log.Fatal(err)
	}
	defer connection.Close()
	creds, err := credentials.NewServerTLSFromFile(*certFile, *keyFile)
	if err != nil {
		log.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(creds), grpc.ForceServerCodec(rawCodec{}), grpc.UnknownServiceHandler(p.grpcProxy(connection, uint32(*grpcPort))), grpc.MaxRecvMsgSize(64<<20), grpc.MaxSendMsgSize(64<<20))
	listen, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", *grpcPort))
	if err != nil {
		log.Fatal(err)
	}
	go func() { log.Fatal(server.Serve(listen)) }()
	for _, entry := range []struct {
		port    int
		handler http.Handler
	}{{*primaryPort, p.httpProxy("primary", *primaryUp)}, {*backupPort, p.httpProxy("backup", *backupUp)}, {*controlPort, http.HandlerFunc(p.control)}} {
		e := entry
		go func() {
			s := &http.Server{Addr: fmt.Sprintf("127.0.0.1:%d", e.port), Handler: e.handler, ReadHeaderTimeout: 10 * time.Second}
			log.Fatal(s.ListenAndServe())
		}()
	}
	select {}
}
