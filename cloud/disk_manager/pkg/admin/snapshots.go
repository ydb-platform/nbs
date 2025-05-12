package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/spf13/cobra"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type commandWithScheduler struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	scheduler    tasks.Scheduler
	ctx          context.Context
	db           *persistence.YDBClient
}

func (t *commandWithScheduler) init() error {
	t.ctx = newContext(t.clientConfig)
	taskStorage, db, err := newTaskStorage(t.ctx, t.serverConfig)
	if err != nil {
		return err
	}

	t.db = db
	logging.Info(t.ctx, "Creating task scheduler")
	taskRegistry := tasks.NewRegistry()

	regularTasksEnabled := false
	t.serverConfig.TasksConfig.RegularSystemTasksEnabled = &regularTasksEnabled
	t.scheduler, err = tasks.NewScheduler(
		t.ctx,
		taskRegistry,
		taskStorage,
		t.serverConfig.TasksConfig,
		metrics.NewEmptyRegistry(),
	)
	if err != nil {
		logging.Error(t.ctx, "Failed to create task scheduler: %v", err)
		return err
	}

	return nil
}

func (t *commandWithScheduler) close() {
	if t.db != nil {
		t.db.Close(t.ctx)
	}
}

func newCommandWithScheduler(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) commandWithScheduler {

	return commandWithScheduler{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}
}

////////////////////////////////////////////////////////////////////////////////

type getSnapshot struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	snapshotID   string
}

func (c *getSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	image, err := resourceStorage.GetSnapshotMeta(ctx, c.snapshotID)
	if err != nil {
		return err
	}

	j, err := json.Marshal(image)
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func newGetSnapshotCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &getSnapshot{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "get",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.snapshotID, "id", "", "ID of snapshot to get; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type listSnapshots struct {
	clientConfig *client_config.ClientConfig
	serverConfig *server_config.ServerConfig
	folderID     string
}

func (c *listSnapshots) run() error {
	ctx := newContext(c.clientConfig)

	resourceStorage, db, err := newResourceStorage(ctx, c.serverConfig)
	if err != nil {
		return err
	}
	defer db.Close(ctx)

	ids, err := resourceStorage.ListSnapshots(ctx, c.folderID, time.Now())
	if err != nil {
		return err
	}

	fmt.Println(strings.Join(ids, "\n"))

	return nil
}

func newListSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	c := &listSnapshots{
		clientConfig: clientConfig,
		serverConfig: serverConfig,
	}

	cmd := &cobra.Command{
		Use: "list",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.folderID,
		"folder-id",
		"",
		"ID of folder where snapshots are located; optional",
	)
	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type createSnapshot struct {
	clientConfig  *client_config.ClientConfig
	zoneID        string
	srcDiskID     string
	dstSnapshotID string
	folderID      string
}

func (c *createSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.CreateSnapshotRequest{
		Src: &disk_manager.DiskId{
			ZoneId: c.zoneID,
			DiskId: c.srcDiskID,
		},
		SnapshotId: c.dstSnapshotID,
		FolderId:   c.folderID,
	}

	resp, err := client.CreateSnapshot(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newCreateSnapshotCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &createSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "create",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.zoneID, "zone-id", "", "zone ID in which disk is located; required")
	if err := cmd.MarkFlagRequired("zone-id"); err != nil {
		log.Fatalf("Error setting flag zone-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.srcDiskID, "src-disk-id", "", "ID of disk to create snapshot from; required")
	if err := cmd.MarkFlagRequired("src-disk-id"); err != nil {
		log.Fatalf("Error setting flag src-disk-id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.dstSnapshotID, "id", "", "ID of snapshot to create; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(&c.folderID, "folder-id", "", "folder ID of the snapshot owner; required")
	if err := cmd.MarkFlagRequired("folder-id"); err != nil {
		log.Fatalf("Error setting flag folder-id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type deleteSnapshot struct {
	clientConfig *client_config.ClientConfig
	snapshotID   string
}

func (c *deleteSnapshot) run() error {
	ctx := newContext(c.clientConfig)

	err := requestConfirmation("snapshot", c.snapshotID)
	if err != nil {
		return err
	}

	client, err := internal_client.NewClient(ctx, c.clientConfig)
	if err != nil {
		return fmt.Errorf("failed to create client: %w", err)
	}
	defer client.Close()

	req := &disk_manager.DeleteSnapshotRequest{
		SnapshotId: c.snapshotID,
	}

	resp, err := client.DeleteSnapshot(getRequestContext(ctx), req)
	if err != nil {
		return err
	}

	fmt.Printf("Operation: %v\n", resp.Id)

	return internal_client.WaitOperation(ctx, client, resp.Id)
}

func newDeleteSnapshotCmd(clientConfig *client_config.ClientConfig) *cobra.Command {
	c := &deleteSnapshot{
		clientConfig: clientConfig,
	}

	cmd := &cobra.Command{
		Use: "delete",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(&c.snapshotID, "id", "", "ID of snapshot to delete; required")
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

// TODO: Remove this command after getting rid of legacy snapshot storage.
type scheduleCreateSnapshotFromLegacySnapshotTask struct {
	commandWithScheduler
	snapshotID string
}

func (c *scheduleCreateSnapshotFromLegacySnapshotTask) run() error {
	err := c.init()
	defer c.close()
	if err != nil {
		return err
	}

	taskID, err := c.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			c.ctx,
			"dataplane.CreateSnapshotFromLegacySnapshot_"+c.snapshotID+"_"+generateID(),
		),
		"dataplane.CreateSnapshotFromLegacySnapshot",
		"",
		&dataplane_protos.CreateSnapshotFromLegacySnapshotRequest{
			SrcSnapshotId: c.snapshotID,
			DstSnapshotId: c.snapshotID,
			UseS3:         true,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Task: %v\n", taskID)
	return nil
}

func newScheduleCreateSnapshotFromLegacySnapshotTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmdWithScheduler := newCommandWithScheduler(clientConfig, serverConfig)
	c := &scheduleCreateSnapshotFromLegacySnapshotTask{
		commandWithScheduler: cmdWithScheduler,
	}

	cmd := &cobra.Command{
		Use: "schedule_create_snapshot_from_legacy_snapshot_task",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to create from legacy snapshot; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type scheduleMigrateSnapshotTaskCmd struct {
	commandWithScheduler
	snapshotID string
}

func (c *scheduleMigrateSnapshotTaskCmd) run() error {
	err := c.init()
	defer c.close()
	if err != nil {
		return err
	}

	taskID, err := c.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			c.ctx,
			"dataplane.MigrateSnapshotTask_"+c.snapshotID+"_"+generateID(),
		),
		"dataplane.MigrateSnapshotTask",
		"",
		&dataplane_protos.MigrateSnapshotRequest{
			SrcSnapshotId: c.snapshotID,
		},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Task: %v\n", taskID)
	return nil
}

func newScheduleMigrateSnapshotTaskCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmdWithScheduler := newCommandWithScheduler(clientConfig, serverConfig)
	c := &scheduleMigrateSnapshotTaskCmd{
		commandWithScheduler: cmdWithScheduler,
	}

	cmd := &cobra.Command{
		Use: "schedule_migrate_snapshot_task",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.snapshotID,
		"id",
		"",
		"ID of snapshot to migrate data to another database; required",
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	return cmd
}

////////////////////////////////////////////////////////////////////////////////

type migrateSnapshotDatabaseCmd struct {
	commandWithScheduler
}

func (c *migrateSnapshotDatabaseCmd) run() error {
	err := c.init()
	defer c.close()
	if err != nil {
		return err
	}

	taskID, err := c.scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(
			c.ctx,
			"dataplane.MigrateSnapshotDatabaseTask_"+generateID(),
		),
		"dataplane.MigrateSnapshotDatabaseTask",
		"",
		&empty.Empty{},
	)
	if err != nil {
		return err
	}

	fmt.Printf("Task: %v\n", taskID)
	return nil
}

func newMigrateSnapshotDatabaseCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmdWithScheduler := newCommandWithScheduler(clientConfig, serverConfig)
	c := &migrateSnapshotDatabaseCmd{
		commandWithScheduler: cmdWithScheduler,
	}

	return &cobra.Command{
		Use: "schedule_migrate_snapshot_database_task",
		RunE: func(cmd *cobra.Command, args []string) error {
			return c.run()
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func newSnapshotsCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	cmd := &cobra.Command{
		Use:     "snapshots",
		Aliases: []string{"snapshot"},
	}

	cmd.AddCommand(
		newGetSnapshotCmd(clientConfig, serverConfig),
		newListSnapshotsCmd(clientConfig, serverConfig),
		newCreateSnapshotCmd(clientConfig),
		newDeleteSnapshotCmd(clientConfig),
		// TODO: Remove this command after getting rid of legacy snapshot storage.
		newScheduleCreateSnapshotFromLegacySnapshotTaskCmd(
			clientConfig,
			serverConfig,
		),
		newScheduleMigrateSnapshotTaskCmd(
			clientConfig,
			serverConfig,
		),
		newMigrateSnapshotDatabaseCmd(
			clientConfig,
			serverConfig,
		),
	)

	return cmd
}
