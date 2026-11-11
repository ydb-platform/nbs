package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	public_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/library/go/core/metrics/solomon"
	"github.com/ydb-platform/nbs/library/go/yandex/solomon/reporters/puller/httppuller"
)

func TestExportMetricsShareSolomonEndpoint(t *testing.T) {
	var mux *http.ServeMux
	var paths []string
	mon := monitoring.NewMonitoring(nil, func(
		m *http.ServeMux,
		path string,
	) metrics.Registry {

		mux = m
		paths = append(paths, path)
		registry := solomon.NewRegistry(solomon.NewRegistryOpts().SetRated(true))
		m.Handle("/solomon/"+path, httppuller.NewHandler(registry))
		return public_metrics.WrapRegistry(registry)
	})
	registry := newSnapshotExportRegistry(&server_config.ServerConfig{
		ExportConfig: &server_config.ExportConfig{},
	}, mon)
	registry.Counter("export/readBytes").Add(1024)
	registry.WithPrefix("s3").WithTags(map[string]string{
		"s3_role": "source", "call": "GetObject", "resource_kind": "all",
	}).Counter("errors").Add(2)
	registry.WithPrefix("s3").WithTags(map[string]string{
		"s3_role": "target", "call": "PutObject", "resource_kind": "snapshot",
	}).Counter("errors").Add(3)
	registry.WithPrefix("snapshot_storage").Counter("count").Inc()

	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/solomon/snapshot_export", nil,
	))
	require.Equal(t, http.StatusOK, response.Code)
	var body struct {
		Sensors []struct {
			Labels map[string]string `json:"labels"`
			Value  int64             `json:"value"`
		} `json:"metrics"`
	}
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &body))
	values := make(map[string]int64)
	for _, sensor := range body.Sensors {
		values[sensor.Labels["sensor"]+":"+sensor.Labels["s3_role"]] = sensor.Value
	}
	require.Equal(t, map[string]int64{
		"export/readBytes:": 1024, "s3.errors:source": 2,
		"s3.errors:target": 3, "snapshot_storage.count:": 1,
	}, values)
	require.Equal(t, []string{"snapshot_export"}, paths)
	for _, path := range []string{"export_source_s3", "export_snapshot_storage"} {
		response := httptest.NewRecorder()
		mux.ServeHTTP(response, httptest.NewRequest(
			http.MethodGet, "/solomon/"+path, nil,
		))
		require.Equal(t, http.StatusNotFound, response.Code)
	}
}

func TestExportMetricsSharePrometheusEndpoint(t *testing.T) {
	var mux *http.ServeMux
	mon := monitoring.NewMonitoring(nil, func(
		m *http.ServeMux,
		path string,
	) metrics.Registry {

		mux = m
		return public_metrics.NewPrometheusRegistry(m, path)
	})
	registry := newSnapshotExportRegistry(&server_config.ServerConfig{
		ExportConfig: &server_config.ExportConfig{},
	}, mon)
	registry.WithPrefix("s3").WithTags(map[string]string{
		"s3_role": "source", "call": "GetObject", "resource_kind": "all",
	}).Counter("errors").Add(2)
	registry.WithPrefix("s3").WithTags(map[string]string{
		"s3_role": "target", "call": "PutObject", "resource_kind": "snapshot",
	}).Counter("errors").Add(3)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, httptest.NewRequest(
		http.MethodGet, "/metrics/", nil,
	))
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `s3_role="source"`)
	require.Contains(t, response.Body.String(), `s3_role="target"`)
}

func TestExportConfigIgnoresRemovedPartsTableFlag(t *testing.T) {
	for _, value := range []string{"true", "false"} {
		t.Run(value, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "export-config.txt")
			err := os.WriteFile(
				path,
				[]byte("UseExportPartsTable: "+value+"\nCheckpointPartCount: 7\n"),
				0600,
			)
			if err != nil {
				t.Fatal(err)
			}

			config := &server_config.ExportConfig{}
			if err := util.ParseProto(path, config); err != nil {
				t.Fatal(err)
			}
			if config.GetCheckpointPartCount() != 7 {
				t.Fatal("known checkpoint settings must remain readable")
			}
		})
	}
}

func TestControlplaneExportDoesNotInitializeExecutor(t *testing.T) {
	for _, exportConfig := range []*server_config.ExportConfig{
		nil,
		{},
		{ForbiddenFolderIds: []string{"forbidden-folder"}},
		{SourceRetryTimeout: proto.String("invalid-worker-setting")},
		{CheckpointInterval: proto.String("invalid-worker-setting")},
		{CheckpointPartCount: proto.Uint32(0)},
		{PartsCleanupBatchSize: proto.Uint32(0)},
	} {
		registry := tasks.NewRegistry()
		cleanup, err := initSnapshotExport(
			context.Background(),
			&server_config.ServerConfig{
				GrpcConfig:   &server_config.GrpcConfig{},
				ExportConfig: exportConfig,
			},
			nil, nil, registry, nil, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		cleanup()

		for _, taskType := range []string{
			"dataplane.ExportSnapshotToS3", "dataplane.ExportImageToS3",
		} {
			if _, err := registry.NewTask(taskType); err != nil {
				t.Fatalf("child state must remain readable: %v", err)
			}
		}

		if len(registry.TaskTypesForExecution()) != 0 {
			t.Fatal("controlplane must not execute transfers")
		}
	}
}

func TestExportExecutorRejectsInvalidCheckpointConfig(t *testing.T) {
	for _, test := range []struct {
		config  *server_config.ExportConfig
		message string
	}{
		{
			&server_config.ExportConfig{
				CheckpointInterval: proto.String("invalid"),
			},
			"interval",
		},
		{
			&server_config.ExportConfig{
				CheckpointInterval: proto.String("0s"),
			},
			"interval",
		},
		{
			&server_config.ExportConfig{
				CheckpointInterval: proto.String("-1s"),
			},
			"interval",
		},
		{
			&server_config.ExportConfig{CheckpointPartCount: proto.Uint32(0)},
			"batch sizes",
		},
		{
			&server_config.ExportConfig{PartsCleanupBatchSize: proto.Uint32(0)},
			"batch sizes",
		},
	} {
		_, err := initSnapshotExport(
			context.Background(),
			&server_config.ServerConfig{ExportConfig: test.config},
			nil, nil, tasks.NewRegistry(), nil, nil,
		)
		if err == nil || !strings.Contains(err.Error(), test.message) {
			t.Fatalf("expected checkpoint validation, got %v", err)
		}
	}
}

func TestExportExecutorRequiresFullConfig(t *testing.T) {
	_, err := initSnapshotExport(
		context.Background(),
		&server_config.ServerConfig{ExportConfig: &server_config.ExportConfig{}},
		nil, nil, tasks.NewRegistry(), nil, nil,
	)
	if err == nil {
		t.Fatal("executor without Compute settings must not start")
	}
}
