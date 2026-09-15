package backup

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup/layout"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

// External S3 bucket that keeps a copy of snapshots.
type Slave struct {
	ID        string
	S3        *persistence.S3Client
	Bucket    string
	KeyPrefix string
}

func (s *Slave) Key(object string) string {
	return layout.Key(s.KeyPrefix, object)
}

////////////////////////////////////////////////////////////////////////////////

type Slaves map[string]*Slave

func (s Slaves) Get(id string) (*Slave, error) {
	slave, ok := s[id]
	if !ok {
		return nil, errors.NewNonRetriableErrorf("unknown backup slave %q", id)
	}

	return slave, nil
}

////////////////////////////////////////////////////////////////////////////////

// Returns nil if backup is not configured.
func NewSlaves(
	config *config.BackupConfig,
	registry metrics.Registry,
	tokenProvider credentials.Credentials,
) (Slaves, error) {

	if config == nil {
		return nil, nil
	}

	if len(config.GetSlaves()) == 0 {
		return nil, errors.NewNonRetriableErrorf("backup slaves are not configured")
	}

	slaves := make(Slaves)
	for _, slaveConfig := range config.GetSlaves() {
		id := slaveConfig.GetId()
		if _, ok := slaves[id]; ok {
			return nil, errors.NewNonRetriableErrorf(
				"backup slave %q is configured twice",
				id,
			)
		}

		s3, err := persistence.NewS3ClientFromConfig(
			slaveConfig.GetS3Config(),
			registry.WithTags(map[string]string{"slave": id}),
			nil, // availabilityMonitoring
			tokenProvider,
		)
		if err != nil {
			return nil, err
		}

		slaves[id] = &Slave{
			ID:        id,
			S3:        s3,
			Bucket:    slaveConfig.GetS3Bucket(),
			KeyPrefix: slaveConfig.GetS3KeyPrefix(),
		}
	}

	_, err := slaves.Get(config.GetDefaultSlave())
	if err != nil {
		return nil, err
	}

	return slaves, nil
}
