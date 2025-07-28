package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	nbsProto "github.com/ydb-platform/nbs/cloud/blockstore/config"
	nbsApiProto "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nfsProto "github.com/ydb-platform/nbs/cloud/filestore/config"
	coreProto "github.com/ydb-platform/nbs/cloud/storage/core/config"
	"github.com/ydb-platform/nbs/cloud/storage/core/tools/common/go/configurator"
	kikimrProto "github.com/ydb-platform/nbs/cloud/storage/core/tools/common/go/configurator/kikimr-proto"
	"gopkg.in/yaml.v2"
)

////////////////////////////////////////////////////////////////////////////////

type Options struct {
	ServicePath       string
	WhiteListClusters string
	ArcadiaRootPath   string
	Verbose           bool
}

////////////////////////////////////////////////////////////////////////////////

func getNbsConfigMap() configurator.ConfigMap {
	return configurator.ConfigMap{
		"nbs-server.txt":              {Proto: &nbsProto.TServerAppConfig{}, FileName: "server.txt"},
		"nbs-client.txt":              {Proto: &nbsProto.TClientAppConfig{}, FileName: "client.txt"},
		"nbs-features.txt":            {Proto: &coreProto.TFeaturesConfig{}, FileName: "features.txt"},
		"nbs-storage.txt":             {Proto: &nbsProto.TStorageServiceConfig{}, FileName: "storage.txt"},
		"nbs-diag.txt":                {Proto: &nbsProto.TDiagnosticsConfig{}, FileName: "diagnostics.txt"},
		"nbs-stats-upload.txt":        {Proto: &nbsProto.TYdbStatsConfig{}, FileName: "ydbstats.txt"},
		"nbs-discovery.txt":           {Proto: &nbsProto.TDiscoveryServiceConfig{}, FileName: "discovery.txt"},
		"nbs-logbroker.txt":           {Proto: &nbsProto.TLogbrokerConfig{}, FileName: "logbroker.txt"},
		"nbs-notify.txt":              {Proto: &nbsProto.TNotifyConfig{}, FileName: "notify.txt"},
		"nbs-disk-registry-proxy.txt": {Proto: &nbsProto.TDiskRegistryProxyConfig{}, FileName: "disk-registry.txt"},
		"nbs-disk-agent.txt":          {Proto: &nbsProto.TDiskAgentConfig{}, FileName: "disk-agent.txt"},
		"nbs-disk-registry.txt":       {Proto: &nbsApiProto.TUpdateDiskRegistryConfigRequest{}, FileName: "update-disk-registry.txt"},
		"nbs-iam.txt":                 {Proto: &coreProto.TIamClientConfig{}, FileName: "iam.txt"},
		"nbs-kms.txt":                 {Proto: &nbsProto.TGrpcClientConfig{}, FileName: "kms.txt"},
		"nbs-compute.txt":             {Proto: &nbsProto.TGrpcClientConfig{}, FileName: "compute.txt"},
		"nbs-rdma.txt":                {Proto: &nbsProto.TRdmaConfig{}, FileName: "rdma.txt"},
		"nbs-root-kms.txt":            {Proto: &nbsProto.TRootKmsConfig{}, FileName: "root-kms.txt"},

		// for kikimr initializer configs used custom protobuf files
		// from cloud/storage/core/tools/common/go/configurator/kikimr-proto
		// with a minimum set of parameters to avoid dependencies
		"nbs-auth.txt":         {Proto: &kikimrProto.TAuthConfig{}, FileName: "auth.txt"},
		"nbs-ic.txt":           {Proto: &kikimrProto.TInterconnectConfig{}, FileName: "ic.txt"},
		"nbs-log.txt":          {Proto: &kikimrProto.TLogConfig{}, FileName: "log.txt"},
		"nbs-shared-cache.txt": {Proto: &kikimrProto.TSharedCacheConfig{}, FileName: "shared-cache.txt"},
		"nbs-sys.txt":          {Proto: &kikimrProto.TActorSystemConfig{}, FileName: "sys.txt"},
	}
}

func getNfsConfigMap() configurator.ConfigMap {
	return configurator.ConfigMap{
		"nfs-server.txt":      {Proto: &nfsProto.TServerAppConfig{}, FileName: "server.txt"},
		"nfs-storage.txt":     {Proto: &nfsProto.TStorageConfig{}, FileName: "storage.txt"},
		"nfs-diag.txt":        {Proto: &nfsProto.TDiagnosticsConfig{}, FileName: "diagnostics.txt"},
		"nfs-vhost.txt":       {Proto: &nfsProto.TVhostAppConfig{}, FileName: "vhost.txt"},
		"nfs-vhost-local.txt": {Proto: &nfsProto.TVhostAppConfig{}, FileName: "vhost-local.txt"},
		"nfs-client.txt":      {Proto: &nfsProto.TClientAppConfig{}, FileName: "client.txt"},
		"nfs-features.txt":    {Proto: &coreProto.TFeaturesConfig{}, FileName: "features.txt"},

		// for kikimr initializer configs used custom protobuf files
		// from cloud/storage/core/tools/common/go/configurator/kikimr-proto
		// with a minimum set of parameters to avoid dependencies
		"nfs-auth.txt":  {Proto: &kikimrProto.TAuthConfig{}, FileName: "auth.txt"},
		"nfs-ic.txt":    {Proto: &kikimrProto.TInterconnectConfig{}, FileName: "ic.txt"},
		"nfs-log.txt":   {Proto: &kikimrProto.TLogConfig{}, FileName: "log.txt"},
		"nfs-sys.txt":   {Proto: &kikimrProto.TActorSystemConfig{}, FileName: "sys.txt"},
		"vhost-log.txt": {Proto: &kikimrProto.TLogConfig{}, FileName: "vhost-log.txt"},
	}
}

func getNfsLocalConfigMap() configurator.ConfigMap {
	return configurator.ConfigMap{
		"nfs-vhost-local.txt": {Proto: &nfsProto.TVhostAppConfig{}, FileName: "vhost.txt"},
		"nfs-diag-local.txt":  {Proto: &nfsProto.TDiagnosticsConfig{}, FileName: "diagnostics.txt"},
	}
}

func getConfigMap(serviceName string) configurator.ConfigMap {
	switch serviceName {
	case "nbs":
		return getNbsConfigMap()
	case "disk_agent", "nbs_disk_agent":
		return getNbsConfigMap()
	case "nfs":
		return getNfsConfigMap()
	case "nfs-local":
		return getNfsLocalConfigMap()
	default:
		return configurator.ConfigMap{}
	}
}

func loadServiceConfig(configPath string) (*configurator.ServiceSpec, error) {
	configTmpl, err := ioutil.ReadFile(path.Join(configPath, "spec.yaml"))
	if err != nil {
		return nil, fmt.Errorf("can't read service config: %w", err)
	}

	tmpl, err := template.New("config").Parse(string(configTmpl))
	if err != nil {
		return nil, fmt.Errorf("can't parse config: %w", err)
	}

	var configYAML strings.Builder

	err = tmpl.Execute(&configYAML, nil)
	if err != nil {
		return nil, fmt.Errorf("can't execute config: %w", err)
	}

	config := &configurator.ServiceSpec{}
	if err := yaml.Unmarshal([]byte(configYAML.String()), config); err != nil {
		return nil, fmt.Errorf("can't unmarshal config: %w", err)
	}

	return config, nil
}

func run(opts *Options, ctx context.Context) error {
	logLevel := nbs.LOG_INFO
	if opts.Verbose {
		logLevel = nbs.LOG_DEBUG
	}

	logger := nbs.NewLog(
		log.New(
			os.Stdout,
			"",
			log.Ltime,
		),
		logLevel,
	)

	var config, err = loadServiceConfig(opts.ServicePath)
	if err != nil {
		return fmt.Errorf("can't load config: %w", err)
	}

	whiteListCluster := make([]string, 0)
	if len(opts.WhiteListClusters) > 0 {
		whiteListCluster = strings.Split(opts.WhiteListClusters, ",")
	}

	return configurator.NewConfigGenerator(
		logger,
		configurator.GeneratorSpec{
			ServiceSpec:   *config,
			ConfigMap:     getConfigMap(config.ServiceName),
			ArcadiaPath:   opts.ArcadiaRootPath,
			OverridesPath: opts.ServicePath}).Generate(ctx, whiteListCluster)
}

func main() {
	var opts Options

	var rootCmd = &cobra.Command{
		Use:   "config-generator",
		Short: "Config generator",
		Run: func(cmd *cobra.Command, args []string) {
			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()

			if err := run(&opts, ctx); err != nil {
				log.Fatalf("Error: %v", err)
			}
		},
	}

	rootCmd.Flags().StringVarP(
		&opts.ServicePath,
		"service-path",
		"s",
		"",
		"path to folder with service spec",
	)

	rootCmd.Flags().StringVarP(
		&opts.WhiteListClusters,
		"whitelist-clusters",
		"c",
		"",
		"specific clusters for generating. Use with service-path",
	)

	rootCmd.Flags().StringVarP(
		&opts.ArcadiaRootPath,
		"arcadia-root-path",
		"a",
		"",
		"path to arcadia root",
	)

	rootCmd.Flags().BoolVarP(&opts.Verbose, "verbose", "v", false, "verbose mode")

	requiredFlags := []string{
		"service-path", "arcadia-root-path",
	}

	for _, flag := range requiredFlags {
		if err := rootCmd.MarkFlagRequired(flag); err != nil {
			log.Fatalf("can't mark flag %v as required: %v", flag, err)
		}
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("can't execute root command: %v", err)
	}
}
