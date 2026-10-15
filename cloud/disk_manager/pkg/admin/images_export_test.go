package admin

import (
	"context"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

func TestImageExportCommand(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
		err  string
	}{
		{"missing ID", nil, `required flag(s) "id"`},
		{"empty ID", []string{"--id="}, "image ID is required"},
		{
			"default source",
			[]string{"--id=image"},
			"dataplane snapshot config is missing",
		},

		{
			"explicit storage",
			[]string{"--id=image", "--source=snapshot-storage"},
			"dataplane snapshot config is missing",
		},

		{
			"backup without disk",
			[]string{"--id=image", "--source=backup"},
			"backup S3 config is missing",
		},

		{
			"disk flag",
			[]string{"--id=image", "--source=backup", "--src-disk-id=disk"},
			"unknown flag: --src-disk-id",
		},

		{
			"invalid source",
			[]string{"--id=image", "--source=other"},
			"--source must be snapshot-storage or backup",
		},

		{
			"empty source",
			[]string{"--id=image", "--source="},
			"--source must be snapshot-storage or backup",
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cmd := newExportImageCmd(&server_config.ServerConfig{})
				cmd.SilenceErrors = true
				cmd.SilenceUsage = true
				cmd.SetArgs(tc.args)
				require.ErrorContains(t, cmd.Execute(), tc.err)
			},
		)
	}
}

func TestImageExportRegistrationAndClientConfigBypass(t *testing.T) {
	group := newImagesCmd(
		&client_config.ClientConfig{},
		&server_config.ServerConfig{},
	)
	cmd, _, err := group.Find([]string{"export"})
	require.NoError(t, err)
	require.Equal(t, "export", cmd.Name())
	require.True(
		t,
		hasCommandAnnotation(cmd, skipClientConfigParsingAnnotation),
	)
	require.Contains(t, cmd.Long, "images/<image-id>/meta.json")
	require.Contains(t, cmd.Long, "snapshot_id")
	require.Nil(t, cmd.Flags().Lookup("src-disk-id"))
	for _, name := range []string{"get", "create", "backup"} {
		cmd, _, err := group.Find([]string{name})
		require.NoError(t, err)
		require.False(
			t,
			hasCommandAnnotation(cmd, skipClientConfigParsingAnnotation),
		)
	}
}

func TestImageExportInvalidBackupConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config *backup_config.SnapshotStorageBackupConfig
		err    string
	}{
		{"absent", nil, "backup S3 config is missing"},
		{
			"missing S3",
			&backup_config.SnapshotStorageBackupConfig{},
			"backup S3 config is missing",
		},

		{
			"missing bucket",
			&backup_config.SnapshotStorageBackupConfig{
				S3Config: &persistence_config.S3Config{},
			},
			"backup S3 bucket is missing",
		},

		{
			"key ID only",
			&backup_config.SnapshotStorageBackupConfig{
				S3Config: &persistence_config.S3Config{},
				S3Bucket: proto.String("source"),
				KekId:    proto.String("key"),
			},
			"set backup kek file and kek id together",
		},

		{
			"key file only",
			&backup_config.SnapshotStorageBackupConfig{
				S3Config: &persistence_config.S3Config{},
				S3Bucket: proto.String("source"),
				KekFile:  proto.String("missing-key"),
			},
			"set backup kek file and kek id together",
		},

		{
			"missing key file",
			&backup_config.SnapshotStorageBackupConfig{
				S3Config: &persistence_config.S3Config{},
				S3Bucket: proto.String("source"),
				KekFile:  proto.String("missing-key"),
				KekId:    proto.String("key"),
			},
			"no such file",
		},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				c := &exportResource{
					kind:       exportImageKind,
					resourceID: "image",
					source:     "backup",
					serverConfig: &server_config.ServerConfig{
						SnapshotStorageBackupConfig: tc.config,
					},
				}

				_, err := c.newBackupSource(context.Background())
				require.ErrorContains(t, err, tc.err)
			},
		)
	}
}
