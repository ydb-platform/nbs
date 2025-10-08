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
	"github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
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
	DriverName                  string
	Endpoint                    string
	NodeID                      string
	VendorVersion               string
	VMMode                      bool
	MonPort                     uint
	NbsHost                     string
	NbsPort                     uint
	NbsSocket                   string
	NfsServerHost               string
	NfsServerPort               uint
	NfsServerSocket             string
	NfsVhostHost                string
	NfsVhostPort                uint
	NfsVhostSocket              string
	SocketsDir                  string
	LocalFilestoreOverridePath  string
	NfsLocalHost                string
	NfsLocalFilestorePort       uint
	NfsLocalFilestoreSocket     string
	NfsLocalEndpointPort        uint
	NfsLocalEndpointSocket      string
	MountOptions                string
	UseDiscardForYDBBasedDisks  bool
	GrpcRequestTimeout          time.Duration
	StartEndpointRequestTimeout time.Duration
}

////////////////////////////////////////////////////////////////////////////////

type Driver struct {
	grpcServer *grpc.Server
	monitoring *monitoring.Monitoring
}

type driverClients struct {
	nbsClientID             string
	nbsClient               client.ClientIface
	nfsFilestoreClient      nfsclient.ClientIface
	nfsEndpointClient       nfsclient.EndpointClientIface
	nfsLocalFilestoreClient nfsclient.ClientIface
	nfsLocalEndpointClient  nfsclient.EndpointClientIface
}

func createClients(cfg Config) (*driverClients, error) {
	nbsClientID := fmt.Sprintf("%s-%s", cfg.DriverName, cfg.NodeID)
	nbsClient, err := nbsclient.NewGrpcClient(
		&nbsclient.GrpcClientOpts{
			Endpoint: getEndpoint(cfg.NbsSocket, cfg.NbsHost, cfg.NbsPort),
			ClientId: nbsClientID,
			Timeout:  &cfg.GrpcRequestTimeout,
		}, nbsclient.NewStderrLog(nbsclient.LOG_DEBUG),
	)
	if err != nil {
		return nil, err
	}

	var nfsFilestoreClient nfsclient.ClientIface
	if cfg.NfsServerSocket != "" || cfg.NfsServerPort != 0 {
		nfsFilestoreClient, err = nfsclient.NewGrpcClient(
			&nfsclient.GrpcClientOpts{
				Endpoint: getEndpoint(cfg.NfsServerSocket, cfg.NfsServerHost, cfg.NfsServerPort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	var nfsLocalFilestoreClient nfsclient.ClientIface
	if cfg.NfsLocalFilestoreSocket != "" || cfg.NfsLocalFilestorePort != 0 {
		nfsLocalFilestoreClient, err = nfsclient.NewGrpcClient(
			&nfsclient.GrpcClientOpts{
				Endpoint: getEndpoint(cfg.NfsLocalFilestoreSocket, cfg.NfsLocalHost, cfg.NfsLocalFilestorePort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	var nfsEndpointClient nfsclient.EndpointClientIface
	if cfg.NfsVhostSocket != "" || cfg.NfsVhostPort != 0 {
		nfsEndpointClient, err = nfsclient.NewGrpcEndpointClient(
			&nfsclient.GrpcClientOpts{
				Endpoint: getEndpoint(cfg.NfsVhostSocket, cfg.NfsVhostHost, cfg.NfsVhostPort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	var nfsLocalEndpointClient nfsclient.EndpointClientIface
	if cfg.NfsLocalEndpointSocket != "" || cfg.NfsLocalEndpointPort != 0 {
		nfsLocalEndpointClient, err = nfsclient.NewGrpcEndpointClient(
			&nfsclient.GrpcClientOpts{
				Endpoint: getEndpoint(cfg.NfsLocalEndpointSocket, cfg.NfsLocalHost, cfg.NfsLocalEndpointPort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	return &driverClients{
		nbsClientID:             nbsClientID,
		nbsClient:               nbsClient,
		nfsFilestoreClient:      nfsFilestoreClient,
		nfsEndpointClient:       nfsEndpointClient,
		nfsLocalFilestoreClient: nfsLocalFilestoreClient,
		nfsLocalEndpointClient:  nfsLocalEndpointClient,
	}, nil
}

func NewDriver(cfg Config) (*Driver, error) {
	externalFsOverrides, err := LoadExternalFsOverrides(cfg.LocalFilestoreOverridePath)
	if err != nil {
		return nil, err
	}

	// Ensure StartEndpointRequestTimeout is set to a value less than
	// GrpcRequestTimeout to prevent issues with dangling endpoints.
	// StartEndpoint may return GRPC_DEADLINE_ERROR however the
	// blockstore-server queues the request and may eventually start the
	// endpoint. As NodeStageVolume fails, Kubernetes will not call
	// NodeUnstageVolume.
	if cfg.StartEndpointRequestTimeout >= cfg.GrpcRequestTimeout {
		return nil,
			fmt.Errorf("Invalid timeout values. StartEndpointRequestTimeout %q must be less than GrpcRequestTimeout %q",
				cfg.StartEndpointRequestTimeout, cfg.GrpcRequestTimeout)
	}

	clients, err := createClients(cfg)
	if err != nil {
		return nil, err
	}

	monitoringCfg := monitoring.MonitoringConfig{
		Port:      cfg.MonPort,
		Path:      "/metrics",
		Component: "server",
	}

	mon := monitoring.NewMonitoring(&monitoringCfg)
	mon.StartListening()
	mon.ReportVersion(cfg.VendorVersion)
	mon.ReportExternalFsMountExpirationTimes(externalFsOverrides.GetMountExpirationTimes())

	errInterceptor := func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {

		method := getCsiMethodName(info.FullMethod)
		mon.ReportRequestReceived(method)

		startTime := time.Now()
		resp, err := handler(ctx, req)
		elapsedTime := time.Since(startTime)

		if err != nil {
			log.WithError(err).WithField("method", info.FullMethod).Error("method failed")
		}
		mon.ReportRequestCompleted(method, err, elapsedTime)
		return resp, err
	}

	grpcServer := grpc.NewServer(grpc.UnaryInterceptor(errInterceptor))

	csi.RegisterIdentityServer(
		grpcServer,
		newIdentityService(cfg.DriverName, cfg.VendorVersion))

	csi.RegisterControllerServer(
		grpcServer,
		newNBSServerControllerService(
			clients.nbsClient,
			clients.nfsFilestoreClient))

	csi.RegisterNodeServer(
		grpcServer,
		newNodeService(
			cfg.NodeID,
			clients.nbsClientID,
			cfg.VMMode,
			cfg.SocketsDir,
			NodeFsTargetPathPattern,
			NodeBlkTargetPathPattern,
			externalFsOverrides,
			clients.nbsClient,
			clients.nfsEndpointClient,
			clients.nfsLocalEndpointClient,
			clients.nfsLocalFilestoreClient,
			mounter.NewMounter(),
			strings.Split(cfg.MountOptions, ","),
			cfg.UseDiscardForYDBBasedDisks,
			cfg.StartEndpointRequestTimeout))

	return &Driver{
		grpcServer: grpcServer,
		monitoring: mon,
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
