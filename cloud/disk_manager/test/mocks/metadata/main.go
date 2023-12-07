package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"

	"github.com/golang/protobuf/proto"
	"github.com/spf13/cobra"
	"github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/metadata/config"
)

////////////////////////////////////////////////////////////////////////////////

type tokenHandler struct {
	serviceConfig *config.MetadataServiceMockConfig
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	ExpiresIn   int64  `json:"expires_in"` // seconds
}

func (h tokenHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	resp := tokenResponse{
		AccessToken: h.serviceConfig.GetAccessToken(),
		ExpiresIn:   12 * 3600,
	}

	bytes, err := json.Marshal(resp)
	if err != nil {
		log.Fatalf("Failed to marshal response %v: %v", resp, err)
	}

	_, err = w.Write(bytes)
	if err != nil {
		log.Fatalf("Failed to write response %v: %v", resp, err)
	}
}

////////////////////////////////////////////////////////////////////////////////

func run(serviceConfig *config.MetadataServiceMockConfig) error {
	lis, err := net.Listen(
		"tcp",
		fmt.Sprintf(":%d", serviceConfig.GetPort()),
	)
	if err != nil {
		return fmt.Errorf(
			"failed to listen on %v: %w",
			serviceConfig.GetPort(),
			err,
		)
	}

	log.Printf("listening on %v", lis.Addr().String())

	mux := http.NewServeMux()
	mux.Handle("/", tokenHandler{serviceConfig: serviceConfig})

	return http.Serve(lis, mux)
}

////////////////////////////////////////////////////////////////////////////////

func parseConfig(
	configFileName string,
	serviceConfig *config.MetadataServiceMockConfig,
) error {

	configBytes, err := os.ReadFile(configFileName)
	if err != nil {
		return fmt.Errorf(
			"failed to read config file %v: %w",
			configFileName,
			err,
		)
	}

	err = proto.UnmarshalText(string(configBytes), serviceConfig)
	if err != nil {
		return fmt.Errorf(
			"failed to parse config file %v as protobuf: %w",
			configFileName,
			err,
		)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func main() {
	log.Println("launching metadata-mock")

	var configFileName string
	serviceConfig := &config.MetadataServiceMockConfig{}

	var rootCmd = &cobra.Command{
		Use:   "metadata-mock",
		Short: "Mock for Compute Metadata Service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return parseConfig(configFileName, serviceConfig)
		},
		Run: func(cmd *cobra.Command, args []string) {
			err := run(serviceConfig)
			if err != nil {
				log.Fatalf("Failed to run: %v", err)
			}
		},
	}

	rootCmd.Flags().StringVar(
		&configFileName,
		"config",
		"metadata-mock-config.txt",
		"Path to the config file",
	)

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
