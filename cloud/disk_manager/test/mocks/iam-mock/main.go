package main

import (
	"fmt"
	"github.com/spf13/cobra"
	iamv1 "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/internal_auth/access_service"
	"github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/iam-mock/iam"
	"google.golang.org/grpc"
	"log"
	"net"
	"net/http"
)

func main() {
	var httpPort int
	var grpcPort int

	var rootCmd = &cobra.Command{
		Use:   "IAM Mock",
		Short: "IAM Mock with an ability to add permissions and users on the flight",
		Run: func(cmd *cobra.Command, args []string) {
			mux := &http.ServeMux{}
			accessService := iam.NewAccessServiceMock(mux)
			iam.NewOauth2TokenService(mux, func(token iam.IssuedTokenType) {
				accessService.AddIssuedToken(token)
			})
			listener, err := net.Listen("tcp", fmt.Sprintf(":%d", grpcPort))
			if err != nil {
				log.Fatalf("failed to listen on %v: %v", grpcPort, err)
			}

			log.Printf("listening on %v", listener.Addr().String())

			srv := grpc.NewServer()
			iamv1.RegisterAccessServiceServer(srv, accessService)

			go func() {
				srv.Serve(listener)
			}()
			http.ListenAndServe(fmt.Sprintf("localhost:%d", httpPort), mux)
		},
	}

	rootCmd.Flags().IntVar(&httpPort, "http-port", 9001, "HTTP port to listen on")
	rootCmd.Flags().IntVar(&grpcPort, "grpc-port", 9002, "gRPC port to listen on")

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error while starting server: %v", err)
	}
}
