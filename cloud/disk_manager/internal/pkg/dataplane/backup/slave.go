package backup

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/layout"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

// External S3 bucket that keeps a copy of snapshots.
type Slave struct {
	S3        *persistence.S3Client
	Bucket    string
	KeyPrefix string
}

func (s *Slave) Key(object string) string {
	return layout.Key(s.KeyPrefix, object)
}

// Returns nil if backup is not configured.
func NewSlave(
	config *config.BackupConfig,
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

	return &Slave{
		S3:        s3,
		Bucket:    config.GetS3Bucket(),
		KeyPrefix: config.GetS3KeyPrefix(),
	}, nil
}
