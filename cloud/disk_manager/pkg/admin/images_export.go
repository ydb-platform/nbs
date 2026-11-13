package admin

import (
	"github.com/spf13/cobra"

	client_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/client/config"
	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
)

////////////////////////////////////////////////////////////////////////////////

const exportImageLong = `Export a ready image from dataplane snapshot storage,
or an image from backup S3, to stdout as a raw disk image stream or to a
user-specified S3 destination. The image ID addresses its own stored data;
creation source IDs are not followed.

` + exportStorageLong + `--source=backup reads images/<image-id>/meta.json and
chunk_maps/<image-id> below SnapshotStorageBackupConfig.S3KeyPrefix. Only --id
is required; no source disk ID is needed. It uses S3Config (Endpoint,
CredentialsFilePath or UseIamToken, Region), S3Bucket, S3KeyPrefix, and
optional KekFile and KekId from --server-config. Set KekFile and KekId
together for encrypted backup objects. AuthConfig is used only when source S3
enables UseIamToken. Other required server-config fields must still parse;
unused service endpoints are not contacted.

Backup mode needs access only to backup S3; no snapshot persistence config,
YDB, NBS, resource database or task scheduler is required. It runs
synchronously and does not enqueue images backup. Missing or invalid metadata,
maps, and referenced chunks fail without falling back to another source.
Backup object envelopes are removed using the configured key; AES-XTS disk
data remains ciphertext.

Direct storage mode checks readiness of the dataplane snapshot with the image
ID; it does not independently verify the control-plane resource type.

` + exportOutputLong

const exportImageExample = `  disk-manager-admin \
      --server-config /etc/disk-manager/server-config.txt \
      images export --id <image-id> > image.raw

  disk-manager-admin \
      --server-config /etc/disk-manager/server-config.txt \
      images export --id <image-id> --source snapshot-storage \
      --s3-endpoint https://destination.example.com \
      --s3-bucket exported-images \
      --s3-credentials-file /path/to/destination-credentials.json

  disk-manager-admin \
      --server-config /etc/disk-manager/server-config.txt \
      images export --id <image-id> --source backup \
      --s3-endpoint https://destination.example.com \
      --s3-bucket exported-images \
      --s3-credentials-file /path/to/destination-credentials.json

  disk-manager-admin --config /path/to/client.txt \
      images export --async --id <image-id> \
      --folder-id <folder-id> --compute-task-id <compute-task-id> \
      --s3-endpoint https://destination.example.com --s3-bucket exported-images`

func newExportImageCmd(
	clientConfig *client_config.ClientConfig,
	serverConfig *server_config.ServerConfig,
) *cobra.Command {

	return newExportCmd(
		clientConfig,
		serverConfig,
		exportImageKind,
		exportImageLong,
		exportImageExample,
	)
}
