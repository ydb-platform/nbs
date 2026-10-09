package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/s3_fault_proxy"
)

func listenLoopback(address string) (net.Listener, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil || host != "127.0.0.1" {
		return nil, fmt.Errorf("listen address must be explicit 127.0.0.1:PORT")
	}
	return net.Listen("tcp4", address)
}

func run() error {
	upstream := flag.String("upstream", "", "loopback HTTP S3 emulator URL")
	listen := flag.String("listen", "", "127.0.0.1:PORT for S3 requests")
	controlListen := flag.String("control-listen", "", "127.0.0.1:PORT for fault control")
	flag.Parse()
	if flag.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	proxy, err := s3_fault_proxy.New(*upstream)
	if err != nil {
		return err
	}
	defer proxy.Close()
	dataListener, err := listenLoopback(*listen)
	if err != nil {
		return err
	}
	defer dataListener.Close()
	controlListener, err := listenLoopback(*controlListen)
	if err != nil {
		return err
	}
	defer controlListener.Close()
	controller := s3_fault_proxy.NewController(proxy)
	defer controller.Close()
	dataServer := &http.Server{
		Handler: proxy, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 30 * time.Second, WriteTimeout: 330 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024,
	}
	controlServer := &http.Server{
		Handler: controller, ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 * 1024,
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errorsCh := make(chan error, 2)
	go func() { errorsCh <- dataServer.Serve(dataListener) }()
	go func() { errorsCh <- controlServer.Serve(controlListener) }()
	select {
	case <-ctx.Done():
	case err = <-errorsCh:
	}
	// Release all holds before waiting for handlers. If an upstream does not
	// finish within this deadline, close the remaining connections explicitly.
	controller.Close()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = controlServer.Shutdown(shutdownCtx)
	_ = dataServer.Shutdown(shutdownCtx)
	_ = controlServer.Close()
	_ = dataServer.Close()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
