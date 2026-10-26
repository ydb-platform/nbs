package admin

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	snapshot_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resource_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	disk_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

func TestExportDiskMetadataSourceDiskLookup(t *testing.T) {
	lookupErr := errors.New("YDB read failed")
	for _, tc := range []struct {
		name      string
		diskID    string
		image     *resources.ImageMeta
		imageErr  error
		disk      *resources.DiskMeta
		diskErr   error
		blockSize uint32
	}{
		{name: "snapshot disk", diskID: "disk", disk: &resources.DiskMeta{BlockSize: 4096}, blockSize: 4096},
		{name: "image disk", image: &resources.ImageMeta{SrcDiskID: "disk"}, disk: &resources.DiskMeta{BlockSize: 512}, blockSize: 512},
		{name: "deleted disk", diskID: "disk"},
		{name: "unknown block size", diskID: "disk", disk: &resources.DiskMeta{}},
		{name: "invalid create request preserves block size", diskID: "disk", disk: &resources.DiskMeta{BlockSize: 4096, CreateRequestData: []byte{0xff}}, blockSize: 4096},
		{name: "unknown image"},
		{name: "image without source disk", image: &resources.ImageMeta{}},
		{name: "disk read error", diskID: "disk", diskErr: lookupErr},
		{name: "image read error", imageErr: lookupErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			storage := resource_mocks.NewStorageMock()
			diskID := tc.diskID
			if diskID == "" {
				storage.On("GetImageMeta", mock.Anything, "snapshot").Return(tc.image, tc.imageErr).Once()
				if tc.image != nil {
					diskID = tc.image.SrcDiskID
				}
			}
			if diskID != "" {
				storage.On("GetDiskMeta", mock.Anything, diskID).Return(tc.disk, tc.diskErr).Once()
			}
			ctx := logging.SetLogger(context.Background(), newExportLogger(logging.InfoLevel))
			metadata, err := readExportDiskMetadata(ctx, storage, "snapshot", tc.diskID)
			if tc.imageErr != nil || tc.diskErr != nil {
				require.ErrorIs(t, err, lookupErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.blockSize, metadata.BlockSize)
			}
			storage.AssertExpectations(t)
		})
	}
}

func TestExportDiskMetadataSkipsYDBWhenUnencryptedOrUnconfigured(t *testing.T) {
	for _, tc := range []struct {
		name       string
		encryption *types.EncryptionDesc
		config     *config.ServerConfig
	}{
		{name: "unencrypted", config: &config.ServerConfig{PersistenceConfig: &persistence_config.PersistenceConfig{}}},
		{name: "no resource config", encryption: &types.EncryptionDesc{Mode: types.EncryptionMode_ENCRYPTION_AES_XTS}, config: &config.ServerConfig{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := snapshot_mocks.NewStorageMock()
			source.On("CheckSnapshotReady", mock.Anything, "snapshot").Return(snapshot_storage.SnapshotMeta{Encryption: tc.encryption}, nil).Once()
			c := &exportResource{resourceID: "snapshot", serverConfig: tc.config}
			ctx := logging.SetLogger(context.Background(), newExportLogger(logging.InfoLevel))
			metadata, err := c.getExportDiskMetadata(ctx, source)
			require.NoError(t, err)
			require.Equal(t, exportDiskMetadata{}, metadata)
			source.AssertExpectations(t)
		})
	}
}

func TestExportDiskEncryptionMetadataFromCreateRequest(t *testing.T) {
	params := &disk_protos.CreateDiskParams{
		BlocksCount: 1024,
		Disk:        &types.Disk{ZoneId: "zone", DiskId: "disk"},
		EncryptionDesc: &types.EncryptionDesc{
			Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
			Key: &types.EncryptionDesc_KmsKey{KmsKey: &types.KmsKey{
				KekId: "kek", EncryptedDEK: []byte{1, 2, 3},
			}},
		},
	}
	for _, tc := range []struct {
		name       string
		request    proto.Message
		imageID    string
		snapshotID string
	}{
		{"empty disk", params, "", ""},
		{"overlay disk", params, "image", ""},
		{"from image", &disk_protos.CreateDiskFromImageRequest{SrcImageId: "image", Params: params}, "image", ""},
		{"from snapshot", &disk_protos.CreateDiskFromSnapshotRequest{SrcSnapshotId: "snapshot", Params: params}, "", "snapshot"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := proto.Marshal(tc.request)
			require.NoError(t, err)
			storage := resource_mocks.NewStorageMock()
			storage.On("GetDiskMeta", mock.Anything, "disk").Return(&resources.DiskMeta{
				ID: "disk", SrcImageID: tc.imageID, SrcSnapshotID: tc.snapshotID,
				BlockSize: 4096, CreateRequestData: data,
			}, nil).Once()
			ctx := logging.SetLogger(context.Background(), newExportLogger(logging.InfoLevel))
			metadata, err := readExportDiskMetadata(ctx, storage, "snapshot", "disk")
			require.NoError(t, err)
			require.Equal(t, exportDiskMetadata{BlockSize: 4096, KekID: "kek", EncryptedDEK: []byte{1, 2, 3}}, metadata)
			storage.AssertExpectations(t)
		})
	}
}

func TestSnapshotExportSourceOptions(t *testing.T) {
	for _, tc := range []struct {
		name   string
		args   []string
		valid  bool
		source string
	}{
		{"default", nil, true, "snapshot-storage"},
		{
			"explicit storage",
			[]string{"--source=snapshot-storage"},
			true,
			"snapshot-storage",
		},

		{
			"backup stdout",
			[]string{"--source=backup", "--src-disk-id=disk"},
			true,
			"backup",
		},

		{
			"backup partition",
			[]string{
				"--source=backup",
				"--src-disk-id=disk",
				"--partition=2",
				"--partition-count=3",
			},
			true,
			"backup",
		},

		{"backup missing disk", []string{"--source=backup"}, false, ""},
		{
			"backup empty disk",
			[]string{"--source=backup", "--src-disk-id="},
			false,
			"",
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
				c := &exportResource{kind: exportSnapshotKind}
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
func TestExportBackupNeedsOnlyS3(t *testing.T) {
	for _, kind := range []exportResourceKind{
		exportSnapshotKind,
		exportImageKind,
	} {
		t.Run(
			string(kind),
			func(t *testing.T) {
				for _, compression := range []string{"lz4", "none"} {
					t.Run(
						compression,
						func(t *testing.T) {
							testExportBackupNeedsOnlyS3(t, kind, compression)
						},
					)
				}
			},
		)
	}
}

func testExportBackupNeedsOnlyS3(
	t *testing.T,
	kind exportResourceKind,
	compression string,
) {

	for _, tc := range []struct {
		name                    string
		encrypted               bool
		key                     string
		fail                    bool
		retry                   bool
		diskMetadataUnavailable bool
	}{
		{name: "unencrypted"},
		{name: "retried PUT", retry: true},
		{"encrypted", true, "correct", false, false, false},
		{"missing key", true, "", true, false, false},
		{"wrong key", true, "wrong", true, false, false},
		{"wrong key id", true, "wrong-id", true, false, false},
		{name: "disk metadata unavailable", diskMetadataUnavailable: true},
	} {
		t.Run(
			tc.name,
			func(t *testing.T) {
				var mu sync.Mutex
				objects := make(map[string]persistence.S3Object)
				var sourceMutations int
				var destinationAttempts int
				var retryBody []byte
				handler := http.HandlerFunc(
					func(w http.ResponseWriter, r *http.Request) {
						credential := "source-id"
						if r.URL.Path == "/destination" ||
							strings.HasPrefix(r.URL.Path, "/destination/") {
							credential = "destination-id"
						}

						if !strings.Contains(
							r.Header.Get("Authorization"),
							"Credential="+credential+"/",
						) {
							t.Errorf("wrong credentials for %v", r.URL.Path)
						}

						if r.Method == http.MethodGet &&
							r.Header.Get("Accept-Encoding") != "identity" {
							t.Error("source GET must preserve stored bytes")
						}

						mu.Lock()
						defer mu.Unlock()
						switch r.Method {
						case http.MethodHead:
							if r.URL.Path != "/source" && r.URL.Path != "/destination" {
								if _, ok := objects[r.URL.Path]; !ok {
									w.WriteHeader(http.StatusNotFound)
									return
								}
							}
							w.WriteHeader(http.StatusOK)
						case http.MethodDelete:
							delete(objects, r.URL.Path)
							if strings.HasPrefix(r.URL.Path, "/source/") {
								sourceMutations++
							}
							w.WriteHeader(http.StatusNoContent)
						case http.MethodPut:
							data, err := io.ReadAll(r.Body)
							if err != nil {
								w.WriteHeader(http.StatusBadRequest)
								return
							}

							if strings.HasPrefix(r.URL.Path, "/destination/part-") {

								assert.Empty(t, r.Header.Get("Content-Encoding"))
								assert.Equal(
									t,
									"application/octet-stream",
									r.Header.Get("Content-Type"),
								)
								sum := sha256.Sum256(data)
								assert.Equal(
									t,
									hex.EncodeToString(sum[:]),
									r.Header.Get("X-Amz-Content-Sha256"),
								)
								md5sum := md5.Sum(data)
								assert.Equal(
									t,
									base64.StdEncoding.EncodeToString(md5sum[:]),
									r.Header.Get("Content-Md5"),
								)
								destinationAttempts++
								if tc.retry {
									if destinationAttempts == 1 {
										retryBody = append([]byte(nil), data...)
									}

									assert.True(t, bytes.Equal(retryBody, data))
									if destinationAttempts <= 4 {
										w.WriteHeader(http.StatusInternalServerError)
										return
									}
								}
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
								Data:     data,
								Metadata: metadata,
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
				)
				newServer := func(bucket string) *httptest.Server {
					return httptest.NewServer(
						http.HandlerFunc(
							func(w http.ResponseWriter, r *http.Request) {
								if r.URL.Path != "/"+bucket &&
									!strings.HasPrefix(
										r.URL.Path,
										"/"+bucket+"/",
									) {
									t.Errorf(
										"wrong endpoint for bucket %v",
										bucket,
									)
								}

								handler.ServeHTTP(w, r)
							},
						),
					)
				}

				server := newServer("source")
				defer server.Close()
				destinationServer := newServer("destination")
				defer destinationServer.Close()

				temp := t.TempDir()
				credentialsFile := filepath.Join(temp, "credentials.json")
				require.NoError(
					t,
					os.WriteFile(
						credentialsFile,
						[]byte(`{"id":"source-id","secret":"source-secret"}`),
						0600,
					),
				)
				destinationCredentials := filepath.Join(
					temp,
					"destination.json",
				)
				require.NoError(
					t,
					os.WriteFile(
						destinationCredentials,
						[]byte(
							`{"id":"destination-id",`+
								`"secret":"destination-secret"}`,
						),
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
					0, // No upload rate limit.
					metrics.NewEmptyRegistry(),
				)
				require.NoError(t, err)
				dek, err := writer.EnsureEncryptedDEK(nil)
				require.NoError(t, err)
				metaKey := backup.SnapshotMetaKey("disk", "resource")
				encryptionMode := uint32(types.EncryptionMode_NO_ENCRYPTION)
				if tc.diskMetadataUnavailable {
					encryptionMode = uint32(types.EncryptionMode_ENCRYPTION_AES_XTS)
				}
				var metaValue interface{} = backup.SnapshotMeta{
					ID:             "resource",
					DiskID:         "disk",
					Size:           backup.ChunkSize,
					EncryptionMode: encryptionMode,
				}

				if kind == exportImageKind {
					metaKey = backup.ImageMetaKey("resource")
					// URL-derived images can have no creation-source IDs.
					metaValue = backup.ImageMeta{
						ID:             "resource",
						Size:           backup.ChunkSize,
						EncryptionMode: encryptionMode,
					}
				}

				meta, err := json.Marshal(metaValue)

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
					newExportLogger(logging.InfoLevel),
				)
				require.NoError(
					t,
					writer.PutObject(
						ctx,
						metaKey,
						dek,
						persistence.S3Object{Data: meta},
					),
				)
				require.NoError(
					t,
					writer.PutObject(
						ctx,
						backup.ChunkMapKey("resource"),
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
					S3Config:    s3Config,
					S3Bucket:    proto.String("source"),
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

				serverConfig := &config.ServerConfig{
					SnapshotStorageBackupConfig: backupConfig,
				}
				unpackFiles := map[string][]byte{}
				if tc.name == "unencrypted" {
					archivePath := filepath.Join(temp, "yc-nbs-export-unpack-0.1.0.tar.gz")
					readmePath := filepath.Join(temp, "instructions.txt")
					archive := []byte{0x1f, 0x8b, 0, 0xff}
					readme := []byte("Инструкция\n")
					require.NoError(t, os.WriteFile(archivePath, archive, 0600))
					require.NoError(t, os.WriteFile(readmePath, readme, 0600))
					serverConfig.ExportConfig = &config.ExportConfig{
						UnpackArchivePath: proto.String(archivePath),
						UnpackReadmePath:  proto.String(readmePath),
					}
					unpackFiles["/destination/yc-nbs-export-unpack-0.1.0.tar.gz"] = archive
					unpackFiles["/destination/yc-nbs-export-unpack-0.1.0.tar.gz.sha256"] = []byte(
						"6adf81ba16d001cb9c84f1bcbe5bb40b4f6b2a11f1c0a79386b9ef1266add294  yc-nbs-export-unpack-0.1.0.tar.gz\n",
					)
					unpackFiles["/destination/README.md"] = readme
				}
				if tc.diskMetadataUnavailable {
					serverConfig.PersistenceConfig = &persistence_config.PersistenceConfig{}
					// Conflicting credentials fail before any YDB connection.
					serverConfig.AuthConfig = &auth_config.AuthConfig{
						MetadataUrl:          proto.String("http://unused"),
						FederatedCredentials: &auth_config.FederatedCredentials{},
					}
				}
				cmd := newExportCmd(serverConfig, kind, "", "")

				args := []string{
					"--id=resource",
					"--source=backup",
					"--s3-endpoint=" + destinationServer.URL,
					"--s3-bucket=destination",
					"--s3-credentials-file=" + destinationCredentials,
					"--s3-part-size=4MiB",
					"--s3-progress-file=" + filepath.Join(temp, "progress.json"),
					"--s3-compression=" + compression,
					"--read-workers=2",
				}

				if kind == exportSnapshotKind {
					args = append(args, "--src-disk-id=disk")
				}

				cmd.SetArgs(args)

				err = cmd.Execute()
				require.Equal(t, 3, sourceMutations)
				if tc.fail {
					require.Error(t, err)
					require.Len(t, objects, 3)
					return
				}

				require.NoError(t, err)
				progressData, err := os.ReadFile(filepath.Join(temp, "progress.json"))
				require.NoError(t, err)
				var progress struct {
					Endpoint string              `json:"endpoint"`
					Manifest exporter.S3Manifest `json:"manifest"`
				}
				require.NoError(t, json.Unmarshal(progressData, &progress))
				require.Equal(t, destinationServer.URL, progress.Endpoint)
				require.Len(t, progress.Manifest.Parts, 1)
				if tc.retry {
					require.Equal(t, 5, destinationAttempts)
				} else {
					require.Equal(t, 1, destinationAttempts)
				}

				require.Len(t, objects, 5+len(unpackFiles))
				for path, data := range unpackFiles {
					require.Equal(t, data, objects[path].Data)
				}
				for path, object := range objects {
					if _, ok := unpackFiles[path]; ok {
						continue
					}
					if strings.HasPrefix(path, "/destination/") {
						if !strings.HasSuffix(path, "manifest.json") {
							decoded := object.Data
							if compression == "lz4" {
								decoded, err = io.ReadAll(
									lz4.NewReader(bytes.NewReader(object.Data)),
								)
								require.NoError(t, err)
							}

							require.True(t, bytes.Equal(data, decoded))
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
							require.Equal(t, "resource", manifest.SnapshotID)
							require.Equal(t, compression, manifest.Compression)
							require.EqualValues(t, 1, manifest.FormatVersion)
							require.Zero(t, manifest.BlockSize)
							require.NotContains(t, string(object.Data), `"block_size"`)
							if tc.diskMetadataUnavailable {
								require.Equal(t, types.EncryptionMode_ENCRYPTION_AES_XTS.String(), manifest.EncryptionMode)
							}
						}
					}
				}

				// Stdout and explicit contiguous partitions share the adapter.
				// Recovery file paths apply only to S3, even when unreadable.
				serverConfig.ExportConfig = &config.ExportConfig{
					UnpackArchivePath: proto.String(filepath.Join(temp, "missing.tar.gz")),
					UnpackReadmePath:  proto.String(filepath.Join(temp, "missing.md")),
				}
				output, err := os.CreateTemp(temp, "raw")
				require.NoError(t, err)
				defer output.Close()
				stdout := os.Stdout
				os.Stdout = output
				defer func() { os.Stdout = stdout }()
				for _, partitionCount := range []int{1, 2} {
					require.NoError(t, output.Truncate(0))
					_, err := output.Seek(0, io.SeekStart)
					require.NoError(t, err)
					for partition := 1; partition <= partitionCount; partition++ {
						rawCmd := newExportCmd(
							&config.ServerConfig{
								SnapshotStorageBackupConfig: backupConfig,
							},
							kind,
							"",
							"",
						)
						rawArgs := []string{
							"--id=resource",
							"--source=backup",
							fmt.Sprintf("--partition=%v", partition),
							fmt.Sprintf("--partition-count=%v", partitionCount),
						}

						if kind == exportSnapshotKind {
							rawArgs = append(rawArgs, "--src-disk-id=disk")
						}

						if partitionCount == 1 {
							rawArgs = rawArgs[:2]
							if kind == exportSnapshotKind {
								rawArgs = append(rawArgs, "--src-disk-id=disk")
							}
						}

						rawCmd.SetArgs(rawArgs)
						require.NoError(t, rawCmd.Execute())
					}

					exported, err := os.ReadFile(output.Name())
					require.NoError(t, err)
					require.Equal(t, data, exported)
				}
			},
		)
	}
}

func TestSnapshotExportBackupMissingConfig(t *testing.T) {
	c := &exportResource{
		kind:            exportSnapshotKind,
		resourceID:      "snapshot",
		serverConfig:    &config.ServerConfig{},
		source:          "backup",
		partition:       1,
		partitionCount:  1,
		readWorkerCount: 1,
	}

	require.ErrorContains(t, c.run(), "backup S3 config is missing")
	c.source = "snapshot-storage"
	require.ErrorContains(
		t,
		c.run(),
		"dataplane snapshot config is missing",
	)
}
