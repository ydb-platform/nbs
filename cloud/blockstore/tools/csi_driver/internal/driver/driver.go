package driver

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	log "github.com/sirupsen/logrus"
	nbsclient "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nfsclient "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type Config struct {
	DriverName    string
	Endpoint      string
	NodeID        string
	VendorVersion string
	VMMode        bool
	NbsPort       uint
	NfsServerPort uint
	NfsVhostPort  uint
	NbsSocketsDir string
	PodSocketsDir string
}

////////////////////////////////////////////////////////////////////////////////

type Driver struct {
	grpcServer *grpc.Server
}

func NewDriver(cfg Config) (*Driver, error) {
	nbsClientID := fmt.Sprintf("%s-%s", cfg.DriverName, cfg.NodeID)
	nbsClient, err := nbsclient.NewGrpcClient(
		&nbsclient.GrpcClientOpts{
			Endpoint: fmt.Sprintf("localhost:%d", cfg.NbsPort),
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
				Endpoint: fmt.Sprintf("localhost:%d", cfg.NfsServerPort),
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
				Endpoint: fmt.Sprintf("localhost:%d", cfg.NfsVhostPort),
			}, nfsclient.NewStderrLog(nfsclient.LOG_DEBUG),
		)
		if err != nil {
			return nil, err
		}
	}

	errInterceptor := func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (interface{}, error) {

		resp, err := handler(ctx, req)
		if err != nil {
			log.WithError(err).WithField("method", info.FullMethod).Error("method failed")
		}
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
			cfg.NbsSocketsDir,
			cfg.PodSocketsDir,
			nbsClient,
			nfsEndpointClient))

	return &Driver{grpcServer: grpcServer}, nil
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
