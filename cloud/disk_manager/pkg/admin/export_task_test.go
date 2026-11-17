package admin

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
)

func TestImageExportTaskPreservesBase64DecryptionInfo(t *testing.T) {
	path := filepath.Join(t.TempDir(), "decryption-info.json")
	err := os.WriteFile(path, []byte(
		`{"kek_id":"key","kek_version":"version","encrypted_dek":"AQID"}`,
	), 0600)
	require.NoError(t, err)
	command := &exportTask{
		kind: exportImageKind, resourceID: "image", folderID: "folder",
		computeTaskID: "compute-task", decryptionInfoFile: path,
		destination: api.ExportDestination{
			Endpoint: "https://s3.example", Bucket: "bucket", Prefix: "prefix",
		},
	}
	message, err := command.request()
	require.NoError(t, err)
	request, ok := message.(*api.ExportImageRequest)
	require.True(t, ok)
	require.Equal(t, "image", request.ImageId)
	require.Equal(t, "folder", request.FolderId)
	require.Equal(t, "prefix", request.Dst.Prefix)
	require.Equal(t, []byte("AQID"), request.DecryptionInfo.EncryptedDek)
	require.Equal(t, "version", request.DecryptionInfo.KekVersion)
}

func TestExportConfigurationSelection(t *testing.T) {
	for _, kind := range []exportResourceKind{exportSnapshotKind, exportImageKind} {
		for _, tc := range []struct {
			name  string
			args  []string
			async bool
		}{
			{name: "default"},
			{name: "async", args: []string{"--async"}, async: true},
			{name: "explicit sync", args: []string{"--async=false"}},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				clientConfig := &client_config.ClientConfig{}
				serverConfig := &server_config.ServerConfig{}
				var group *cobra.Command
				if kind == exportSnapshotKind {
					group = newSnapshotsCmd(clientConfig, serverConfig)
				} else {
					group = newImagesCmd(clientConfig, serverConfig)
				}
				cmd, _, err := group.Find([]string{"export"})
				require.NoError(t, err)
				require.Equal(t, "export", cmd.Name())
				require.NoError(t, cmd.ParseFlags(tc.args))
				require.Equal(t, tc.async, needsClientConfig(cmd))
				require.Equal(t, !tc.async, needsServerConfig(cmd))
				for _, child := range group.Commands() {
					require.NotEqual(t, "export-task", child.Name())
				}
			})
		}
	}
}

func TestExportModeValidation(t *testing.T) {
	for _, kind := range []exportResourceKind{exportSnapshotKind, exportImageKind} {
		for _, tc := range []struct {
			name string
			args []string
			err  string
		}{
			{
				name: "async needs metadata",
				args: []string{"--async"},
				err:  "--folder-id, --compute-task-id",
			},
			{
				name: "async rejects local credentials",
				args: []string{"--async", "--s3-credentials-file=creds.json"},
				err:  "--s3-credentials-file cannot be used with --async",
			},
			{
				name: "async rejects local source",
				args: []string{"--async", "--source=backup"},
				err:  "--source cannot be used with --async",
			},
			{
				name: "async rejects local workers",
				args: []string{"--async", "--s3-upload-workers=1"},
				err:  "--s3-upload-workers cannot be used with --async",
			},
			{
				name: "wait needs async",
				args: []string{"--wait"},
				err:  "--wait requires --async",
			},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				cmd := newExportCmd(
					nil, &server_config.ServerConfig{}, kind, "", "",
				)
				cmd.SilenceErrors = true
				cmd.SilenceUsage = true
				cmd.SetArgs(append([]string{"--id=resource"}, tc.args...))
				require.ErrorContains(t, cmd.Execute(), tc.err)
			})
		}
	}
}
