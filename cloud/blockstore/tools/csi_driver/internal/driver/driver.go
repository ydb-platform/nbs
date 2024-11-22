package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi"
	log "github.com/sirupsen/logrus"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/monitoring"
	"github.com/ydb-platform/nbs/cloud/blockstore/tools/csi_driver/internal/mounter"
	nfsclient "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

func getEndpoint(unixSocket, host string, port uint) string {
	if unixSocket != "" {
		return fmt.Sprintf("unix://%s", unixSocket)
	} else {
		return fmt.Sprintf("%s:%d", host, port)
	}
}

func getCsiMethodName(fullMethodName string) string {
	// take the last part from "/csi.v1.Controller/ControllerPublishVolume"
	index := strings.LastIndex(fullMethodName, "/")
	if index > 0 {
		return fullMethodName[index+1:]
	}
	return fullMethodName
}

////////////////////////////////////////////////////////////////////////////////

type Config struct {
	DriverName      string
	Endpoint        string
	NodeID          string
	VendorVersion   string
	VMMode          bool
	MonPort         uint
	NbsHost         string
	NbsPort         uint
	NbsSocket       string
	NfsServerHost   string
	NfsServerPort   uint
	NfsServerSocket string
	NfsVhostHost    string
	NfsVhostPort    uint
	NfsVhostSocket  string
	SocketsDir      string
}

////////////////////////////////////////////////////////////////////////////////

type Driver struct {
	grpcServer *grpc.Server
	monitoring *monitoring.Monitoring
}

func NewDriver(cfg Config) (*Driver, error) {
	nbsClientID := fmt.Sprintf("%s-%s", cfg.DriverName, cfg.NodeID)
	nbsClient, err := nbsclient.NewGrpcClient(
		&nbsclient.GrpcClientOpts{
			Endpoint: getEndpoint(cfg.NbsSocket, cfg.NbsHost, cfg.NbsPort),
			ClientId: nbsClientID,
		}, nbsclient.NewStderrLog(nbsclient.LOG_DEBUG),
	)
	if err != nil {
		return nil, err
	}

	var nfsClient nfsclient.ClientIface
	if cfg.NfsServerPort != 0 {
		nfsClient, err = nfsclient.NewGrpcClient(
			&nfsclient.GrpcClientOpts{
				Endpoint: getEndpoint(cfg.NfsServerSocket, cfg.NfsServerHost, cfg.NfsServerPort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	var nfsEndpointClient nfsclient.EndpointClientIface
	if cfg.NfsVhostPort != 0 {
		nfsEndpointClient, err = nfsclient.NewGrpcEndpointClient(
			&nfsclient.GrpcClientOpts{
				Endpoint: getEndpoint(cfg.NfsVhostSocket, cfg.NfsVhostHost, cfg.NfsVhostPort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	monintoringCfg := monitoring.MonitoringConfig{
		Port:      cfg.MonPort,
		Path:      "/metrics",
		Component: "server",
	}

	monintoring := monitoring.NewMonitoring(&monintoringCfg)
	monintoring.StartListening()
	monintoring.ReportVersion(cfg.VendorVersion)

	errInterceptor := func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {

		method := getCsiMethodName(info.FullMethod)
		monintoring.ReportRequestReceived(method)

		startTime := time.Now()
		resp, err := handler(ctx, req)
		elapsedTime := time.Since(startTime)

		if err != nil {
			log.WithError(err).WithField("method", info.FullMethod).Error("method failed")
		}
		monintoring.ReportRequestCompleted(method, err, elapsedTime)
		return resp, err
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(errInterceptor))

	csi.RegisterIdentityServer(
		grpcServer,
		newIdentityService(cfg.DriverName, cfg.VendorVersion))

	csi.RegisterControllerServer(
		grpcServer,
		newNBSServerControllerService(nbsClient, nfsClient))

	csi.RegisterNodeServer(
		grpcServer,
		newNodeService(
			cfg.NodeID,
			nbsClientID,
			cfg.VMMode,
			cfg.SocketsDir,
			NodeFsTargetPathPattern,
			NodeBlkTargetPathPattern,
			nbsClient,
			nfsEndpointClient,
			mounter.NewMounter()))

	return &Driver{
		grpcServer: grpcServer,
		monitoring: monintoring,
	}, nil
}

func (s *Driver) Run(socketPath string) error {
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		return err
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, syscall.SIGINT, syscall.SIGTERM)
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGPIPE)

	var wg sync.WaitGroup
	defer wg.Wait()

	defer s.grpcServer.GracefulStop()

	done := make(chan error, 1)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer log.Print("Server finished")

		log.Print("Server started")
		if err := s.grpcServer.Serve(listener); err != nil {
			done <- err
		}
	}()

	select {
	case err := <-done:
		if err != nil {
			log.Printf("Server failed: %+v", err)
		}
	case sig := <-signalChannel:
		log.Printf("Got unix signal %q", sig)
	}

	return nil
}
