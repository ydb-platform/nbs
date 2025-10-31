package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/ydb/public/api/client/yc_private/compute/inner"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

type computeService struct {
}

func (s *computeService) CreateToken(
	ctx context.Context,
	req *compute.CreateTokenRequest,
) (*compute.CreateTokenResponse, error) {

	log.Printf("get CreateToken request: %v", req)
	return &compute.CreateTokenResponse{
		Token: "token-from-compute-mock",
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func run(port uint32) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return fmt.Errorf("failed to listen on %v: %w", port, err)
	}

	log.Printf("listening on %v", lis.Addr().String())

	srv := grpc.NewServer()
	compute.RegisterDiskServiceServer(srv, &computeService{})

	return srv.Serve(lis)
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	log.Println("launching compute-mock")

	var port uint32
	var rootCmd = &cobra.Command{
		Use:   "compute-mock",
		Short: "Mock for Compute",
		Run: func(cmd *cobra.Command, args []string) {
			err := run(port)
			if err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}
	rootCmd.Flags().Uint32Var(&port, "port", 4201, "server port")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
