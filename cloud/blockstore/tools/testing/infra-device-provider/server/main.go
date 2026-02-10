package main

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	pb "github.com/ydb-platform/nbs/cloud/blockstore/tools/testing/infra-device-provider/protos"
	"gopkg.in/yaml.v3"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type device struct {
	PCIeAddress  string `yaml:"pci"`
	SerialNumber string `yaml:"serial"`
}

type config struct {
	Devices []device `yaml:"devices"`
}

////////////////////////////////////////////////////////////////////////////////

type InfraService struct {
	pb.UnimplementedTInfraServiceServer

	mu      sync.RWMutex
	devices []*pb.TDevice
}

func (s *InfraService) ListDevices(
	ctx context.Context,
	req *pb.TListDevicesRequest,
) (*pb.TListDevicesResponse, error) {

	log.Printf("Processing ListDevices request...")

	s.mu.Lock()
	defer s.mu.Unlock()

	resp := &pb.TListDevicesResponse{
		Devices: s.devices,
	}

	return resp, nil
}

////////////////////////////////////////////////////////////////////////////////

func loadDevices(path string) ([]*pb.TDevice, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	devices := make([]*pb.TDevice, 0, len(cfg.Devices))
	for _, d := range cfg.Devices {
		devices = append(devices, &pb.TDevice{
			PCIeAddress:  d.PCIeAddress,
			SerialNumber: d.SerialNumber,
		})
	}
	return devices, nil
}

func run(socketPath string, devicesPath string) error {
	devices, err := loadDevices(devicesPath)
	if err != nil {
		return fmt.Errorf("Failed to load devices: %v", err)
	}

	log.Println("Loaded devices:", devices)

	_ = os.Remove(socketPath)

	var stop = make(chan struct{})

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		sig := <-signalChan
		log.Printf("Received signal: %v, shutting down...", sig)
		close(stop)
	}()

	log.Printf("Starting Infra on socket %s", socketPath)

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to start listen socket %q: %v\n", socketPath, err)
	}

	server := grpc.NewServer()
	pb.RegisterTInfraServiceServer(server, &InfraService{
		devices: devices,
	})

	go func() {
		err := server.Serve(listener)
		log.Fatalf("Failed to start gRPC server: %v\n", err)
	}()

	log.Println("Infra started, waiting for shutdown signal...")

	<-stop

	server.GracefulStop()

	log.Println("Infra server stopped successfully")

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	var socketPath string
	var devicesPath string

	var rootCmd = &cobra.Command{
		Use:   "server",
		Short: "Infra service",
		Run: func(cmd *cobra.Command, args []string) {
			err := run(socketPath, devicesPath)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}

	rootCmd.Flags().StringVar(&socketPath, "socket", "", "Path to a socket")
	rootCmd.Flags().StringVar(&devicesPath, "devices", "", "Path to a devices file")

	if err := rootCmd.MarkFlagRequired("socket"); err != nil {
		panic(err)
	}

	if err := rootCmd.MarkFlagRequired("devices"); err != nil {
		panic(err)
	}

	_ = rootCmd.Execute()
}
