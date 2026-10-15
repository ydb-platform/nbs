package admin

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	logzap "github.com/ydb-platform/nbs/library/go/core/log/zap"
)

////////////////////////////////////////////////////////////////////////////////

type exportResourceKind string

const (
	exportSnapshotKind exportResourceKind = "snapshot"
	exportImageKind    exportResourceKind = "image"
)

type exportResource struct {
	kind              exportResourceKind
	serverConfig      *server_config.ServerConfig
	source            string
	srcDiskID         string
	resourceID        string
	readWorkerCount   int
	partition         uint32
	partitionCount    uint32
	verbose           bool
	s3Mode            bool
	s3ProgressFile    string
	s3Endpoint        string
	s3Bucket          string
	s3CredentialsFile string
	s3Region          string
	s3Prefix          string
	s3Compression     string
	s3PartSize        string
	s3RequestTimeout  time.Duration
	s3MaxRetries      int
	s3UploadWorkers   int
	parsedS3PartSize  uint64
}

func newExportLogger(level logging.Level) logging.Logger {
	config := logzap.ConsoleConfig(level)
	config.OutputPaths = []string{"stderr"}
	return logzap.Must(config)
}

func (c *exportResource) run() error {
	if err := exporter.ValidateReadWorkers(c.readWorkerCount); err != nil {
		return err
	}

	if err := exporter.ValidatePartition(
		c.partition,
		c.partitionCount,
	); err != nil {
		return err
	}

	level := logging.InfoLevel
	if c.verbose {
		level = logging.DebugLevel
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()
	ctx = logging.SetLogger(
		ctx,
		newExportLogger(level),
	)

	var destination *persistence.S3Client
	if c.s3Mode {
		credentials, err := persistence.NewS3CredentialsFromFile(
			c.s3CredentialsFile,
		)
		if err != nil {
			return fmt.Errorf("loading destination S3 credentials: %w", err)
		}

		if credentials.ID == "" || credentials.Secret == "" {
			return fmt.Errorf(
				"destination S3 credentials must contain nonempty " +
					"id and secret",
			)
		}

		destination, err = persistence.NewS3Client(
			c.s3Endpoint,
			c.s3Region,
			credentials,
			c.s3RequestTimeout,
			metrics.NewEmptyRegistry(),
			0, // The exporter applies the retry limit across fresh call timeouts.
			nil,
			nil,
			uint64(c.s3UploadWorkers), // Max idle connections per host.
			persistence.WithS3EmptySigningRegion(),
		)
		if err != nil {
			return fmt.Errorf("initializing destination S3 client: %w", err)
		}
	}

	source, closeSource, err := c.newSource(ctx)
	if err != nil {
		return err
	}
	defer closeSource()

	if c.s3Mode {
		diskMetadata, err := c.getExportDiskMetadata(ctx, source)
		if err != nil {
			logging.Warn(ctx, "failed to obtain disk metadata; exporting backup without it: %v", err)
			diskMetadata = exportDiskMetadata{}
		}

		logging.Info(ctx, "exporting %v %v to S3", c.kind, c.resourceID)
		exportConfig := c.serverConfig.GetExportConfig()
		_, err = exporter.ExportToS3(
			ctx,
			source,
			c.resourceID,
			destination,
			exporter.S3ExportOptions{
				MaxRetries:        c.s3MaxRetries,
				ProgressFile:      c.s3ProgressFile,
				SourceIdentity:    fmt.Sprintf("%s/%s/%s", c.kind, c.source, c.srcDiskID),
				Endpoint:          c.s3Endpoint,
				Bucket:            c.s3Bucket,
				Prefix:            c.s3Prefix,
				Compression:       c.s3Compression,
				BlockSize:         diskMetadata.BlockSize,
				KekID:             diskMetadata.KekID,
				EncryptedDEK:      diskMetadata.EncryptedDEK,
				UnpackArchivePath: exportConfig.GetUnpackArchivePath(),
				UnpackReadmePath:  exportConfig.GetUnpackReadmePath(),
				PartSize:          c.parsedS3PartSize,
				ReadWorkerCount:   c.readWorkerCount,
				UploadWorkerCount: c.s3UploadWorkers,
			},
		)
		return err
	}

	stats, err := exporter.ExportPartitionToWriterWithReadWorkers(
		ctx,
		source,
		c.resourceID,
		os.Stdout,
		c.partition,
		c.partitionCount,
		c.readWorkerCount,
	)
	if err != nil {
		return err
	}

	logging.Info(
		ctx,
		"exported partition %v/%v of %v %v to stdout: "+
			"size %v bytes, %v data chunks, %v zero chunks",
		c.partition,
		c.partitionCount,
		c.kind,
		c.resourceID,
		stats.Size,
		stats.DataChunkCount,
		stats.ZeroChunkCount,
	)
	return nil
}

func newExportCmd(
	serverConfig *server_config.ServerConfig,
	kind exportResourceKind,
	long string,
	example string,
) *cobra.Command {

	c := &exportResource{
		kind:            kind,
		serverConfig:    serverConfig,
		source:          "snapshot-storage",
		readWorkerCount: exporter.DefaultStreamReadWorkerCount,
		partition:       1,
		partitionCount:  1,
	}

	cmd := &cobra.Command{
		Use: "export",
		Short: fmt.Sprintf(
			"Exports a raw %v to stdout or an S3 backup prefix",
			kind,
		),
		Long:    long,
		Example: example,
		Annotations: map[string]string{
			skipClientConfigParsingAnnotation: "true",
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := c.validateExportOptions(cmd); err != nil {
				return err
			}

			// Runtime failures are not command-line usage errors.
			cmd.SilenceUsage = true
			return c.run()
		},
	}

	cmd.Flags().StringVar(
		&c.resourceID,
		"id",
		"",
		fmt.Sprintf("ID of %v to export; required", kind),
	)
	if err := cmd.MarkFlagRequired("id"); err != nil {
		log.Fatalf("Error setting flag id as required: %v", err)
	}

	cmd.Flags().StringVar(
		&c.source,
		"source",
		"snapshot-storage",
		"Source: snapshot-storage or backup",
	)
	if kind == exportSnapshotKind {
		cmd.Flags().StringVar(
			&c.srcDiskID,
			"src-disk-id",
			"",
			"Original disk ID; required with --source=backup, "+
				"otherwise rejected",
		)
	}

	cmd.Flags().IntVar(
		&c.readWorkerCount,
		"read-workers",
		exporter.DefaultStreamReadWorkerCount,
		"Parallel chunk readers; stdout buffers up to workers * 4 chunks of 4MiB",
	)
	cmd.Flags().Uint32Var(
		&c.partition,
		"partition",
		1,
		"1-based partition number to export",
	)
	cmd.Flags().Uint32Var(
		&c.partitionCount,
		"partition-count",
		1,
		fmt.Sprintf("Total number of contiguous %v partitions", kind),
	)
	cmd.Flags().BoolVarP(
		&c.verbose,
		"verbose",
		"v",
		false,
		"Enable verbose logging",
	)
	cmd.Flags().StringVar(
		&c.s3ProgressFile,
		"s3-progress-file",
		"",
		"Local checkpoint file; save completed parts and resume when it already exists",
	)
	cmd.Flags().StringVar(
		&c.s3Endpoint,
		"s3-endpoint",
		"",
		"Destination S3-compatible HTTP(S) endpoint",
	)
	cmd.Flags().StringVar(
		&c.s3Bucket,
		"s3-bucket",
		"",
		"Existing destination bucket",
	)
	cmd.Flags().StringVar(
		&c.s3CredentialsFile,
		"s3-credentials-file",
		"",
		"Destination credentials JSON: id, secret, optional session_token",
	)
	cmd.Flags().StringVar(
		&c.s3Region,
		"s3-region",
		"",
		"Signature V4 region required by the destination provider",
	)
	cmd.Flags().StringVar(
		&c.s3Prefix,
		"s3-prefix",
		"",
		"Destination key prefix; empty writes directly to the bucket root",
	)
	cmd.Flags().StringVar(
		&c.s3Compression,
		"s3-compression",
		exporter.DefaultS3Compression,
		"Destination part compression: lz4 or none; explicit flag selects S3",
	)
	cmd.Flags().StringVar(
		&c.s3PartSize,
		"s3-part-size",
		"256MiB",
		"Maximum raw part size: multiple of 4MiB, at most 1GiB",
	)
	cmd.Flags().DurationVar(
		&c.s3RequestTimeout,
		"s3-request-timeout",
		5*time.Minute,
		"Positive timeout per destination call attempt",
	)
	cmd.Flags().IntVar(
		&c.s3UploadWorkers,
		"s3-upload-workers",
		exporter.DefaultS3UploadWorkerCount,
		"Compression/upload workers (1-64); defaults use ~1.25GiB payload buffers",
	)
	cmd.Flags().IntVar(
		&c.s3MaxRetries,
		"s3-max-retries",
		-1,
		"Maximum retries per source/destination operation; -1 retries indefinitely (default)",
	)

	return cmd
}

const exportStorageLong = `This command does not call the Disk Manager service
and does not require a valid client config. The default
--source=snapshot-storage uses --server-config and reads
DataplaneConfig.SnapshotConfig plus AuthConfig from that file. The host must
have network access to dataplane YDB and, when configured, S3. Read-only
access is enough: the command reads snapshot metadata, chunk maps, chunk blobs
and S3 objects.

`

const exportOutputLong = `The stored chunk map is complete, including shared
chunks inherited from an incremental snapshot or image creation source. The
exporter reads chunk bytes as stored and does not decrypt them. AES-XTS
resources with key-hash metadata remain ciphertext. Resources encrypted at
rest with a root-KMS-provided key are exported as plaintext logical disk data.
Legacy snapshot storage configured by LegacyStorageFolder is not supported.

The output stream is not sparse: zero chunks are written explicitly because
stdout is sequential. Up to --read-workers * 4 chunk reads/results can be
pending, each using a 4 MiB buffer, plus the selected partition chunk map.
With the default --partition-count=1 this is the whole resource map; use
--partition-count and concatenate partitions in ascending order to bound map
memory for large snapshots in snapshot-storage mode. Backup mode always loads
the entire protobuf chunk map, including when exporting a partition, in
addition to the output buffers.

S3 mode requires --s3-endpoint, --s3-bucket and --s3-credentials-file. Source
storage still comes from --server-config; the destination is independent. The
default --s3-max-retries=-1 retries retryable source reads and destination
operations indefinitely with backoff, until interrupted or terminated. Each
destination call has its own --s3-request-timeout; reaching that timeout does
not exhaust unlimited retries. Permanent errors fail immediately. Set
--s3-max-retries to a nonnegative value to limit retries per source or
destination operation after its initial attempt; 0 disables exporter retries.
Destination SDK retries are disabled so they do not multiply this limit.
Source clients retain the SDK retry settings from --server-config.
The credentials JSON contains id, secret and an optional session_token. HTTPS
validates certificates; HTTP is supported for local endpoints. The optional
--s3-region defaults to empty and controls signing only; set the region
required by your provider. The bucket must already exist, with permission to
check it, write objects and read back an uncertain manifest upload.

S3 mode exports the whole resource and rejects explicit --partition and
--partition-count flags. --s3-part-size defaults to 256MiB, must be a positive
multiple of 4MiB, and cannot exceed 1GiB. --s3-upload-workers defaults to 2
and accepts 1 through 64 concurrent compression/upload workers. Reading the
next part overlaps with uploads. Multiple parts are assembled concurrently so
one slow chunk does not stall assembly of the other parts in the buffer pool.
--read-workers limits
active chunk reads across all parts together. At most upload-workers + 1 raw
part buffers and one encoded buffer per worker are retained. LZ4 output buffers
are capped at the raw part length plus 64KiB, including incompressible frames.
Default payload capacity is about 1.25GiB, plus encoder workspaces, source/SDK
buffers, loaded maps and metadata. Smaller parts or fewer upload workers reduce
memory use. With --s3-compression=none, defaults allow up to 768MiB of raw part
buffers. Chunk readers decode directly into independent slices of a part. Only
the final partial chunk needs an extra 4MiB decode buffer to verify its full
checksum. Part buffers are retained throughout SDK retries. Payload signing
reuses the SHA-256 already computed for the manifest.
In snapshot-storage mode, non-final YDB chunk map reads are bounded to the
current part's chunk range, avoiding prefetch of the remaining parts. The final
part also checks for corrupt map entries beyond the resource's chunk count.
Objects are placed directly under --s3-prefix, or in the bucket root when the
prefix is empty. Keys are part-00000001.raw.lz4,
part-00000002.raw.lz4, ... and manifest.json. --s3-compression defaults to lz4;
each object contains one standard LZ4 frame with 4MiB blocks and a content
checksum. Use --s3-compression=none for uncompressed part-00000001.raw objects.
An explicit --s3-compression selects S3 mode and requires the full destination;
its implicit default preserves raw stdout. HTTP Content-Encoding is unset.
Only the manifest marks completion. Format version 1 uses snapshot_id for the
dataplane snapshot ID, including when that ID belongs to an image. The required
compression field selects lz4 or none. Each part's offset and length describe
logical bytes; stored_length and sha256 describe exact object bytes. Download
parts in manifest order, verify stored lengths and SHA-256 hashes, then decode
LZ4 frames (or copy none parts), verify logical lengths and concatenate to
reconstruct the raw image. Serialized manifests are limited to 256MiB.
For encrypted resources only, the optional manifest field block_size contains
the source disk's logical block size in bytes, read from the resource YDB disks
table using PersistenceConfig, AuthConfig and DisksConfig.StorageFolder in
--server-config. This requires read access to resource YDB when
PersistenceConfig is present. For images created from disks,
ImagesConfig.StorageFolder is also used to find the source disk. If resource
YDB is not configured or the source disk metadata is unavailable, block_size
is omitted. Unencrypted resources do not query resource YDB and omit block_size.
The same disks row supplies create_request. Its CreateDiskParams.EncryptionDesc
KMS key, when present, is exported as kek_id and encrypted_dek (base64-encoded).
The decoded encrypted_dek bytes are also stored in encrypted.dek beside
manifest.json. The kek_id is also stored as plain text in kek.id. Both files
are uploaded before the completion manifest when their values are present.
Missing or unreadable create_request omits the key fields and preserves
block_size. Failures to obtain disk metadata are logged as warnings; the export
continues with those optional fields omitted.
Optional ExportConfig.UnpackArchivePath and ExportConfig.UnpackReadmePath in
--server-config specify local recovery utility archive and README files.
For example:
ExportConfig {
    UnpackArchivePath: "/opt/yc-nbs-export-unpack-0.1.0.tar.gz"
    UnpackReadmePath: "/opt/yc-nbs-export-unpack/README.md"
}
The archive is uploaded in the bucket root using its original filename,
including any version, without local directory components. The README is
uploaded as README.md in the bucket root. These files are shared by all export
prefixes and uploaded on every export with PUT, overwriting existing objects
without checking, comparing or deleting them first.
When an archive is configured, its SHA-256 is computed
from the bytes to be uploaded before export and written to <archive-name>.sha256
in the bucket root in sha256sum format, using the original archive filename.
The checksum object is also uploaded with PUT on every export. Local
files are read before changing destination objects; any uploads happen before
the completion manifest.
A read or upload failure, including the checksum upload, fails the export.
Unset paths omit the corresponding uploads; stdout export ignores these paths.
The optional manifest field encryption_key_hash contains the
base64-encoded disk key hash alongside encryption_mode and
raw_data_interpretation; no secret key is stored. AES-XTS ciphertext requires
the original disk key and a compatible restoration or decryption path to
access the filesystem. Root-KMS metadata is normally recorded as
NO_ENCRYPTION; the source metadata is preserved.

The backup UUID is recorded only in the manifest. Repeated exports overwrite
part objects at the same keys. An existing manifest.json is removed before
uploading parts, so a failed overwrite has no completion marker. Overwriting
requires permission to delete that manifest. A new manifest is published only
after all parts succeed. Old part objects not referenced by the new manifest
are retained. Do not run concurrent exports to the same destination prefix.
Failed attempts leave their uploaded
parts for explicit inspection or cleanup; there is no automatic part deletion.
With --s3-progress-file=PATH, completed parts are saved atomically to a local
JSON checkpoint after each successful upload. Rerun with the same file and
source/destination settings to resume; completed parts are skipped, and missing
objects are uploaded again. The file is retained after success. Source data and
existing destination objects must remain unchanged between attempts; existing
parts are trusted without downloading them to recheck hashes. Do not share a
checkpoint file between concurrent exports. Changed source metadata, endpoint,
bucket, prefix, compression or part size is rejected before destination writes.
Read/upload worker counts may change. A checkpoint write failure stops the
export without publishing a completion manifest. Without this flag, every run
starts from scratch. Remove the checkpoint to start a fresh export.

Keep the source resource or backup stable and available for the duration; no deletion-prevention lease is acquired and concurrent cleanup or
re-backup is not coordinated. A readiness check does not prevent chunk-map
deletion. Progress and destination URIs are printed to stderr. The raw image
includes stored padding and does not recreate an imported QCOW2, VMDK or VHD
container. Every 5 seconds, S3 progress reports logical bytes read,
completed logical bytes against resource size, confirmed stored bytes, the last
started part, stage, active reads, compression workers and PUTs, assembling and
ready parts, completed parts, read speed and stored transfer speed in MiB/s.
Completion advances once per successful PUT. The ratio is stored/logical bytes
for the same completed parts. Each completed part reports its stored copy speed,
including reads, compression, upload and retries.`
