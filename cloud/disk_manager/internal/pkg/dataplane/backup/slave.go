package backup

import (
	"context"
	"fmt"
	"strings"

	backup_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

// External S3 bucket that keeps a copy of snapshots. Owns the key layout:
//
//	chunks/<chunk_id>                            chunk object, copied as is
//	snapshots/<disk_id>/<snapshot_id>/meta.json  snapshot meta, written first
//	snapshots/<disk_id>/<snapshot_id>/map.bin    chunk map, written last
//
// Tasks put objects by their meaning and do not see the keys.
type Slave struct {
	s3        *persistence.S3Client
	bucket    string
	keyPrefix string
}

// Returns nil if backup is not configured.
func NewSlave(
	config *backup_config.BackupConfig,
	registry metrics.Registry,
	tokenProvider credentials.Credentials,
) (*Slave, error) {

	if config == nil {
		return nil, nil
	}

	s3, err := persistence.NewS3ClientFromConfig(
		config.GetS3Config(),
		registry,
		nil, // availabilityMonitoring
		tokenProvider,
	)
	if err != nil {
		return nil, err
	}

	return newSlave(s3, config.GetS3Bucket(), config.GetS3KeyPrefix()), nil
}

func (s *Slave) PutChunk(
	ctx context.Context,
	chunkID string,
	object persistence.S3Object,
) error {

	return s.putObject(ctx, chunkObject(chunkID), object)
}

func (s *Slave) PutMeta(
	ctx context.Context,
	diskID string,
	snapshotID string,
	data []byte,
) error {

	return s.putObject(
		ctx,
		metaObject(diskID, snapshotID),
		persistence.S3Object{Data: data},
	)
}

func (s *Slave) PutMap(
	ctx context.Context,
	diskID string,
	snapshotID string,
	data []byte,
) error {

	return s.putObject(
		ctx,
		mapObject(diskID, snapshotID),
		persistence.S3Object{Data: data},
	)
}

////////////////////////////////////////////////////////////////////////////////

func newSlave(
	s3 *persistence.S3Client,
	bucket string,
	keyPrefix string,
) *Slave {

	return &Slave{
		s3:        s3,
		bucket:    bucket,
		keyPrefix: strings.TrimSuffix(keyPrefix, "/"),
	}
}

func (s *Slave) putObject(
	ctx context.Context,
	object string,
	s3Object persistence.S3Object,
) error {

	return s.s3.PutObject(ctx, s.bucket, s.key(object), s3Object)
}

// Objects are keys relative to the prefix.
func (s *Slave) key(object string) string {
	if len(s.keyPrefix) == 0 {
		return object
	}

	return fmt.Sprintf("%v/%v", s.keyPrefix, object)
}

func chunkObject(chunkID string) string {
	return fmt.Sprintf("chunks/%v", chunkID)
}

func metaObject(diskID string, snapshotID string) string {
	return fmt.Sprintf("%v/meta.json", snapshotDir(diskID, snapshotID))
}

func mapObject(diskID string, snapshotID string) string {
	return fmt.Sprintf("%v/map.bin", snapshotDir(diskID, snapshotID))
}

func snapshotDir(diskID string, snapshotID string) string {
	if len(diskID) == 0 {
		diskID = "-"
	}

	return fmt.Sprintf("snapshots/%v/%v", diskID, snapshotID)
}
