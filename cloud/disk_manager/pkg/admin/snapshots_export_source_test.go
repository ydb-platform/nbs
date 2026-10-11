package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"

	config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

func TestSnapshotExportSourceOptions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		valid  bool
		source string
	}{
		{"default", nil, true, "snapshot-storage"},
		{
			"explicit storage", []string{"--source=snapshot-storage"},
			true, "snapshot-storage",
		},

		{
			"backup stdout", []string{"--source=backup", "--src-disk-id=disk"},
			true, "backup",
		},

		{
			"backup partition", []string{
				"--source=backup", "--src-disk-id=disk",
				"--partition=2", "--partition-count=3",
			}, true, "backup",
		},

		{"backup missing disk", []string{"--source=backup"}, false, ""},
		{
			"backup empty disk", []string{"--source=backup", "--src-disk-id="},
			false, "",
		},

		{"disk for storage", []string{"--src-disk-id=disk"}, false, ""},
		{"empty disk for storage", []string{"--src-disk-id="}, false, ""},
		{"invalid source", []string{"--source=image"}, false, ""},
		{"empty source", []string{"--source="}, false, ""},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				cmd := newExportSnapshotCmd(&config.ServerConfig{})
				require.NoError(t, cmd.ParseFlags(tc.args))
				c := &exportSnapshot{}
				c.source, _ = cmd.Flags().GetString("source")
				c.srcDiskID, _ = cmd.Flags().GetString("src-disk-id")
				err := c.validateSourceOptions(cmd)
				if tc.valid {
					require.NoError(t, err)
					require.Equal(t, tc.source, c.source)
				} else {
					require.Error(t, err)
				}
			},
		)
	}
}

// A local object endpoint exercises the real persistence and backup clients.
// Only GET is allowed on the source during export.
func TestSnapshotExportBackupNeedsOnlyS3(t *testing.T) {
	for _, tc := range []struct {
		name      string
		encrypted bool
		key       string
		fail      bool
	}{
		{"unencrypted", false, "", false},
		{"encrypted", true, "correct", false},
		{"missing key", true, "", true},
		{"wrong key", true, "wrong", true},
		{"wrong key id", true, "wrong-id", true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				var mu sync.Mutex
				objects := make(map[string]persistence.S3Object)
				var sourceMutations int
				server := httptest.NewServer(
					http.HandlerFunc(
						func(w http.ResponseWriter, r *http.Request) {
							mu.Lock()
							defer mu.Unlock()
							switch r.Method {
							case http.MethodHead:
								w.WriteHeader(http.StatusOK)
							case http.MethodPut:
								data, err := io.ReadAll(r.Body)
								if err != nil {
									w.WriteHeader(http.StatusBadRequest)
									return
								}

								metadata := make(map[string]*string)
								for name := range r.Header {
									if strings.HasPrefix(name, "X-Amz-Meta-") {
										value := r.Header.Get(name)
										name = strings.TrimPrefix(
											name,
											"X-Amz-Meta-",
										)
										metadata[name] = &value
									}
								}

								objects[r.URL.Path] = persistence.S3Object{
									Data: data, Metadata: metadata,
								}

								if strings.HasPrefix(r.URL.Path, "/source/") {
									sourceMutations++
								}

							case http.MethodGet:
								object, ok := objects[r.URL.Path]
								if !ok {
									w.WriteHeader(http.StatusNotFound)
									return
								}

								for name, value := range object.Metadata {
									w.Header().Set("X-Amz-Meta-"+name, *value)
								}

								_, _ = w.Write(object.Data)
							default:
								w.WriteHeader(http.StatusMethodNotAllowed)
							}
						},
					),
				)
				defer server.Close()

				temp := t.TempDir()
				credentialsFile := filepath.Join(temp, "credentials.json")
				require.NoError(
					t,
					os.WriteFile(
						credentialsFile,
						[]byte(`{"id":"test","secret":"test"}`),
						0600,
					),
				)
				s3Config := &persistence_config.S3Config{
					Endpoint:            proto.String(server.URL),
					Region:              proto.String("ru-central1"),
					CredentialsFilePath: proto.String(credentialsFile),
				}

				s3, err := persistence.NewS3ClientFromConfig(
					s3Config,
					metrics.NewEmptyRegistry(),
					nil,
					nil,
				)
				require.NoError(t, err)
				key := bytes.Repeat([]byte{1}, 32)
				var sourceKey []byte
				var sourceKeyID string
				if tc.encrypted {
					sourceKey, sourceKeyID = key, "key"
				}

				writer, err := backup.NewS3(
					s3,
					"source",
					"prefix",
					sourceKeyID,
					sourceKey,
					0, // uploadBytesPerSecond
					metrics.NewEmptyRegistry(),
				)
				require.NoError(t, err)
				dek, err := writer.EnsureEncryptedDEK(nil)
				require.NoError(t, err)
				meta, err := json.Marshal(
					backup.SnapshotMeta{
						ID: "snapshot", DiskID: "disk", Size: backup.ChunkSize,
					},
				)

				require.NoError(t, err)
				chunkMap, err := proto.Marshal(
					&protos.BackupChunkMap{
						ChunkIds: []string{"chunk"},
					},
				)

				require.NoError(t, err)
				data := bytes.Repeat([]byte{3}, backup.ChunkSize)
				ctx := logging.SetLogger(
					context.Background(),
					newExportSnapshotLogger(logging.InfoLevel),
				)
				require.NoError(
					t,
					writer.PutObject(
						ctx,
						backup.SnapshotMetaKey("disk", "snapshot"),
						dek,
						persistence.S3Object{Data: meta},
					),
				)
				require.NoError(
					t,
					writer.PutObject(
						ctx,
						backup.ChunkMapKey("snapshot"),
						dek,
						persistence.S3Object{Data: chunkMap},
					),
				)
				require.NoError(
					t,
					writer.PutObject(
						ctx,
						backup.ChunkKey("chunk"),
						dek,
						chunks.NewS3Object(
							chunks.ChunkBlob{
								Data:     data,
								Checksum: common.Chunk{Data: data}.Checksum(),
							},
						),
					),
				)

				backupConfig := &backup_config.SnapshotStorageBackupConfig{
					S3Config: s3Config, S3Bucket: proto.String("source"),
					S3KeyPrefix: proto.String("prefix"),
				}

				if tc.key != "" {
					keyID := "key"
					if tc.key == "wrong" {
						key = bytes.Repeat([]byte{2}, 32)
					}

					if tc.key == "wrong-id" {
						keyID = "other"
					}

					keyFile := filepath.Join(temp, "key")
					require.NoError(t, os.WriteFile(keyFile, key, 0600))
					backupConfig.KekFile = proto.String(keyFile)
					backupConfig.KekId = proto.String(keyID)
				}

				// No dataplane, YDB, resource, NBS, task or auth configuration.
				cmd := newExportSnapshotCmd(
					&config.ServerConfig{
						SnapshotStorageBackupConfig: backupConfig,
					},
				)

				cmd.SetArgs(
					[]string{
						"--id=snapshot",
						"--source=backup",
						"--src-disk-id=disk",
						"--s3-endpoint=" + server.URL,
						"--s3-bucket=destination",
						"--s3-credentials-file=" + credentialsFile,
						"--s3-part-size=4MiB", "--read-workers=2",
					},
				)

				err = cmd.Execute()
				mu.Lock()
				defer mu.Unlock()
				require.Equal(t, 3, sourceMutations)
				if tc.fail {
					require.Error(t, err)
					require.Len(t, objects, 3)
					return
				}

				require.NoError(t, err)
				require.Len(t, objects, 5)
				for path, object := range objects {
					if strings.HasPrefix(path, "/destination/") {
						if strings.HasSuffix(path, ".raw") {
							require.True(t, bytes.Equal(data, object.Data))
						} else {
							var manifest exporter.S3Manifest
							require.NoError(
								t,
								json.Unmarshal(object.Data, &manifest),
							)
							require.EqualValues(
								t,
								backup.ChunkSize,
								manifest.Size,
							)
						}
					}
				}
			},
		)
	}
}

func TestSnapshotExportBackupMissingConfig(t *testing.T) {
	c := &exportSnapshot{
		serverConfig: &config.ServerConfig{}, source: "backup",
		partition: 1, partitionCount: 1, readWorkerCount: 1,
	}

	require.ErrorContains(t, c.run(), "backup S3 config is missing")
	c.source = "snapshot-storage"
	require.ErrorContains(t, c.run(), "dataplane snapshot config is missing")
}
