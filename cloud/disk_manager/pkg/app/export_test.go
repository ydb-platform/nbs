package app

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

func TestControlplaneExportDoesNotInitializeExecutor(t *testing.T) {
	for _, exportConfig := range []*server_config.ExportConfig{
		nil,
		{},
		{ForbiddenFolderIds: []string{"forbidden-folder"}},
		{SourceRetryTimeout: proto.String("invalid-worker-setting")},
	} {
		registry := tasks.NewRegistry()
		cleanup, err := initSnapshotExport(
			context.Background(),
			&server_config.ServerConfig{
				GrpcConfig:   &server_config.GrpcConfig{},
				ExportConfig: exportConfig,
			},
			nil, nil, registry, nil,
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

func TestExportExecutorRequiresFullConfig(t *testing.T) {
	_, err := initSnapshotExport(
		context.Background(),
		&server_config.ServerConfig{ExportConfig: &server_config.ExportConfig{}},
		nil, nil, tasks.NewRegistry(), nil,
	)
	if err == nil {
		t.Fatal("executor without Compute settings must not start")
	}
}
