package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/cobra"
)

////////////////////////////////////////////////////////////////////////////////

const (
	// Matches the S3 PutObject StorageClass header.
	storageClassHeader = "x-amz-storage-class"
	quotaErrCode       = "QuotaLimitExceeded"
)

type quotaProxy struct {
	proxy *httputil.ReverseProxy

	mu    sync.Mutex
	quota map[string]uint64
}

type quotaReservationKey struct{}

type quotaReservation struct {
	storageClass string
	size         uint64
}

var errQuotaExceeded = errors.New("Quota limit exceeded")
var errUnknownStorageClass = errors.New("Unknown storage class")

////////////////////////////////////////////////////////////////////////////////

func newQuotaProxy(s3URL *url.URL, quota map[string]uint64) *quotaProxy {
	p := &quotaProxy{
		quota: quota,
	}

	proxy := &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(s3URL)
			r.Out.Host = s3URL.Host
		},
		ModifyResponse: func(resp *http.Response) error {
			if resp.StatusCode < http.StatusBadRequest {
				return nil
			}

			p.releaseReservation(resp.Request.Context())
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			p.releaseReservation(r.Context())
			http.Error(w, err.Error(), http.StatusBadGateway)
		},
	}
	p.proxy = proxy

	return p
}

func normalizeStorageClass(class string) string {
	return strings.ToUpper(strings.TrimSpace(class))
}

func parseQuotaSpecs(specs []string) map[string]uint64 {
	quota := make(map[string]uint64)

	for _, spec := range specs {
		class, sizeStr, _ := strings.Cut(spec, ":")
		size, _ := strconv.ParseUint(strings.TrimSpace(sizeStr), 10, 64)
		quota[normalizeStorageClass(class)] = size
	}

	return quota
}

func replyClientError(w http.ResponseWriter, code string, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(
		w,
		"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"+
			"<Error><Code>%s</Code><Message>%s</Message></Error>",
		code,
		message,
	)
}

func (p *quotaProxy) reserve(
	storageClass string,
	size uint64,
) (*quotaReservation, error) {

	storageClass = normalizeStorageClass(storageClass)
	if len(storageClass) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	quota, ok := p.quota[storageClass]
	if !ok {
		if storageClass == "STANDARD" || storageClass == "" {
			return nil, nil
		}

		return nil, errUnknownStorageClass
	}

	if size > quota {
		return nil, errQuotaExceeded
	}

	p.quota[storageClass] -= size
	return &quotaReservation{
		storageClass: storageClass,
		size:         size,
	}, nil
}

func (p *quotaProxy) releaseReservation(ctx context.Context) {
	reservation, _ := ctx.Value(quotaReservationKey{}).(*quotaReservation)
	if reservation == nil {
		return
	}

	p.release(reservation.storageClass, reservation.size)
}

func (p *quotaProxy) release(storageClass string, size uint64) {
	storageClass = normalizeStorageClass(storageClass)
	if len(storageClass) == 0 {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.quota[storageClass] += size
}

func (p *quotaProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		p.proxy.ServeHTTP(w, r)
		return
	}

	if r.ContentLength < 0 {
		http.Error(w, "missing content length", http.StatusLengthRequired)
		return
	}

	storageClass := r.Header.Get(storageClassHeader)
	size := uint64(r.ContentLength)

	reservation, err := p.reserve(storageClass, size)
	if err == nil {
		if reservation != nil {
			ctx := context.WithValue(
				r.Context(),
				quotaReservationKey{},
				reservation,
			)
			r = r.WithContext(ctx)
		}

		p.proxy.ServeHTTP(w, r)

		return
	}

	if errors.Is(err, errQuotaExceeded) {
		replyClientError(
			w,
			quotaErrCode,
			"Quota limit exceeded",
		)
		return
	}

	replyClientError(
		w,
		"unknown_error",
		fmt.Sprintf("Unknown error: %v", err),
	)
}

////////////////////////////////////////////////////////////////////////////////

func run(
	ctx context.Context,
	port string,
	s3Port string,
	quotaSpecs []string,
) error {

	s3URL, err := url.Parse("http://localhost:" + s3Port)
	if err != nil {
		return fmt.Errorf("failed to build S3 URL: %w", err)
	}

	quota := parseQuotaSpecs(quotaSpecs)

	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("failed to listen on %v: %w", port, err)
	}
	defer lis.Close()

	log.Printf(
		"listening on %v, s3 %v, quotas %v",
		lis.Addr().String(),
		s3URL,
		quota,
	)

	server := &http.Server{
		Handler: newQuotaProxy(s3URL, quota),
	}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()

	err = server.Serve(lis)
	if err == http.ErrServerClosed {
		return nil
	}

	return err
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	log.Println("launching s3quota-proxy")

	var port string
	var s3Port string
	var quotaSpecs []string

	rootCmd := &cobra.Command{
		Use:   "s3-quota-proxy",
		Short: "S3 quota proxy mock",
		Run: func(cmd *cobra.Command, args []string) {
			err := run(context.Background(), port, s3Port, quotaSpecs)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}
	rootCmd.Flags().StringVar(&port, "port", "", "server port")
	rootCmd.Flags().StringVar(&s3Port, "s3-port", "", "S3 port")
	rootCmd.Flags().StringArrayVar(
		&quotaSpecs,
		"quota",
		nil,
		"storage class quota in class:size format",
	)
	if err := rootCmd.MarkFlagRequired("port"); err != nil {
		log.Fatalf("Error setting flag port as required: %v", err)
	}

	if err := rootCmd.MarkFlagRequired("s3-port"); err != nil {
		log.Fatalf("Error setting flag s3-port as required: %v", err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
