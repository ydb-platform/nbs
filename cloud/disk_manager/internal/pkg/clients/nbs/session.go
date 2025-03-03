package nbs

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
)

////////////////////////////////////////////////////////////////////////////////

type Session struct {
	nbs                 *nbs_client.DiscoveryClient
	metricsRegistry     metrics.Registry
	diskID              string
	mountOpts           nbs_client.MountVolumeOpts
	rediscoverPeriodMin time.Duration
	rediscoverPeriodMax time.Duration
	clientID            string

	mutex            sync.RWMutex
	client           *nbs_client.Client
	session          *nbs_client.Session
	metrics          *sessionMetrics
	volume           *protos.TVolume
	cancelRediscover func()
}

func generateClientID() (string, error) {
	uuid, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s@%s", uuid, hostname), nil
}

func newSession(
	ctx context.Context,
	nbs *nbs_client.DiscoveryClient,
	metricsRegistry metrics.Registry,
	diskID string,
	mountOpts nbs_client.MountVolumeOpts,
	rediscoverPeriodMin time.Duration,
	rediscoverPeriodMax time.Duration,
) (session *Session, err error) {

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.newSession",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString(
				"access_mode",
				protos.EVolumeAccessMode_name[int32(mountOpts.AccessMode)],
			),
			tracing.AttributeString(
				"mount_mode",
				protos.EVolumeMountMode_name[int32(mountOpts.MountMode)],
			),
			tracing.AttributeInt("mount_flags", int(mountOpts.MountFlags)),
			tracing.AttributeInt64(
				"mount_seq_number",
				int64(mountOpts.MountSeqNumber),
			),
			tracing.AttributeInt64(
				"fill_generation",
				int64(mountOpts.FillGeneration),
			),
			tracing.AttributeInt64(
				"fill_seq_number",
				int64(mountOpts.FillSeqNumber),
			),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	if mountOpts.EncryptionSpec != nil {
		span.SetAttributes(
			tracing.AttributeString(
				"encryption_mode",
				protos.EEncryptionMode_name[int32(mountOpts.EncryptionSpec.Mode)],
			),
		)
	}

	clientID, err := generateClientID()
	if err != nil {
		return nil, err
	}

	span.SetAttributes(tracing.AttributeString("client_id", clientID))

	session = &Session{
		nbs:                 nbs,
		metricsRegistry:     metricsRegistry,
		diskID:              diskID,
		mountOpts:           mountOpts,
		rediscoverPeriodMin: rediscoverPeriodMin,
		rediscoverPeriodMax: rediscoverPeriodMax,
		clientID:            clientID,
	}

	err = session.init(ctx)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func NewROSession(
	ctx context.Context,
	nbs *nbs_client.DiscoveryClient,
	metricsRegistry metrics.Registry,
	diskID string,
	mountFlags uint32,
	encryptionSpec *protos.TEncryptionSpec,
	rediscoverPeriodMin time.Duration,
	rediscoverPeriodMax time.Duration,
) (*Session, error) {

	mountOpts := nbs_client.MountVolumeOpts{
		MountFlags:     mountFlags,
		EncryptionSpec: encryptionSpec,
		AccessMode:     protos.EVolumeAccessMode_VOLUME_ACCESS_READ_ONLY,
		MountMode:      protos.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
	}
	return newSession(
		ctx,
		nbs,
		metricsRegistry,
		diskID,
		mountOpts,
		rediscoverPeriodMin,
		rediscoverPeriodMax,
	)
}

func NewLocalROSession(
	ctx context.Context,
	nbs *nbs_client.DiscoveryClient,
	metricsRegistry metrics.Registry,
	diskID string,
	mountFlags uint32,
	encryptionSpec *protos.TEncryptionSpec,
	rediscoverPeriodMin time.Duration,
	rediscoverPeriodMax time.Duration,
) (*Session, error) {

	mountOpts := nbs_client.MountVolumeOpts{
		MountFlags:     mountFlags,
		EncryptionSpec: encryptionSpec,
		AccessMode:     protos.EVolumeAccessMode_VOLUME_ACCESS_READ_ONLY,
		MountMode:      protos.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
	}
	return newSession(
		ctx,
		nbs,
		metricsRegistry,
		diskID,
		mountOpts,
		rediscoverPeriodMin,
		rediscoverPeriodMax,
	)
}

func NewRWSession(
	ctx context.Context,
	nbs *nbs_client.DiscoveryClient,
	metricsRegistry metrics.Registry,
	diskID string,
	fillGeneration uint64,
	fillSeqNumber uint64,
	mountFlags uint32,
	encryptionSpec *protos.TEncryptionSpec,
	rediscoverPeriodMin time.Duration,
	rediscoverPeriodMax time.Duration,
) (*Session, error) {

	// We use local mount here for saving one network hop.
	mountOpts := nbs_client.MountVolumeOpts{
		FillGeneration: fillGeneration,
		FillSeqNumber:  fillSeqNumber,
		MountFlags:     mountFlags,
		EncryptionSpec: encryptionSpec,
		AccessMode:     protos.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		MountMode:      protos.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
	}
	return newSession(
		ctx,
		nbs,
		metricsRegistry,
		diskID,
		mountOpts,
		rediscoverPeriodMin,
		rediscoverPeriodMax,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *Session) init(ctx context.Context) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	rediscoverCtx, cancel := context.WithCancel(context.Background())
	rediscoverCtx = logging.SetLogger(rediscoverCtx, logging.GetLogger(ctx))
	s.cancelRediscover = cancel

	go func() {
		for {
			rediscoverPeriod := common.RandomDuration(
				s.rediscoverPeriodMin,
				s.rediscoverPeriodMax,
			)

			select {
			case <-rediscoverCtx.Done():
				return
			case <-time.After(rediscoverPeriod):
			}

			s.rediscover(rediscoverCtx)
		}
	}()

	volume, err := s.discoverAndMount(ctx)
	if err != nil {
		s.closeImpl(ctx)
		return err
	}

	s.volume = volume
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *Session) BlockSize() uint32 {
	return s.volume.BlockSize
}

func (s *Session) BlockCount() uint64 {
	return s.volume.BlocksCount
}

func (s *Session) IsOverlayDisk() bool {
	return len(s.volume.BaseDiskId) != 0
}

func (s *Session) EncryptionDesc() (*types.EncryptionDesc, error) {
	return getEncryptionDesc(s.volume.EncryptionDesc)
}

func (s *Session) IsDiskRegistryBasedDisk() bool {
	return isDiskRegistryBasedDisk(s.volume.StorageMediaKind)
}

func (s *Session) Read(
	ctx context.Context,
	startIndex uint64,
	blockCount uint32,
	checkpointID string,
	data []byte,
	zero *bool,
) (err error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.session == nil {
		return errors.NewRetriableErrorf("last rediscover was failed")
	}

	defer s.metrics.StatRequest("Read")(&err)

	blocks, err := s.session.ReadBlocks(
		s.withClientID(ctx),
		startIndex,
		blockCount,
		checkpointID,
	)
	if err != nil {
		return wrapError(err)
	}

	if nbs_client.AllBlocksEmpty(blocks) {
		*zero = true
		return nil
	}

	return nbs_client.JoinBlocks(s.BlockSize(), blockCount, blocks, data)
}

func (s *Session) Write(
	ctx context.Context,
	startIndex uint64,
	data []byte,
) (err error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.session == nil {
		return errors.NewRetriableErrorf("last rediscover was failed")
	}

	blocksCount := uint32(len(data)) / s.BlockSize()
	logging.Debug(
		ctx,
		"WriteBlocks on disk %v, offset %v, blocks count %v",
		s.diskID,
		startIndex,
		blocksCount,
	)

	defer s.metrics.StatRequest("Write")(&err)

	err = s.session.WriteBlocks(
		s.withClientID(ctx),
		startIndex,
		[][]byte{data},
	)
	return wrapError(err)
}

func (s *Session) Zero(
	ctx context.Context,
	startIndex uint64,
	blockCount uint32,
) (err error) {

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.session == nil {
		return errors.NewRetriableErrorf("last rediscover was failed")
	}

	defer s.metrics.StatRequest("Zero")(&err)

	err = s.session.ZeroBlocks(
		s.withClientID(ctx),
		startIndex,
		blockCount,
	)
	return wrapError(err)
}

func (s *Session) Close(ctx context.Context) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.closeImpl(ctx)
}

////////////////////////////////////////////////////////////////////////////////

// Not thread-safe.
func (s *Session) closeImpl(ctx context.Context) {
	s.closeSession(ctx)
	// Should do cancelRediscover after closing the session, otherwise
	// UnmountVolume (which is called by closeSession) may fail immediately.
	// Note: this problem occurs only when closeImpl is called by rediscover.
	s.cancelRediscover()
}

// Not thread-safe.
func (s *Session) discoverAndMount(ctx context.Context) (*protos.TVolume, error) {
	client, host, err := s.nbs.DiscoverInstance(ctx)
	if err != nil {
		return nil, wrapError(err)
	}

	s.client = client
	s.session = nbs_client.NewSession(
		*client,
		NewNbsClientLog(nbs_client.LOG_DEBUG),
	)
	s.metrics = newSessionMetrics(s.metricsRegistry, host)

	err = s.session.MountVolume(
		s.withClientID(ctx),
		s.diskID,
		&s.mountOpts,
	)
	if err != nil {
		return nil, wrapError(err)
	}

	return s.session.Volume(), nil
}

func (s *Session) rediscover(ctx context.Context) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if ctx.Err() != nil {
		return
	}

	s.closeSession(ctx)

	_, err := s.discoverAndMount(ctx)
	if err != nil {
		logging.Warn(ctx, "rediscover failed: %v", err)
		// Stop subsequent rediscovers.
		s.closeImpl(ctx)
		return
	}

	logging.Info(ctx, "rediscovered for disk %v", s.diskID)
}

// Not thread-safe.
func (s *Session) closeSession(ctx context.Context) {
	if s.session == nil {
		return
	}

	_ = s.session.UnmountVolume(s.withClientID(ctx))
	s.session.Close()
	_ = s.client.Close()

	s.session = nil
	s.client = nil
}

func (s *Session) withClientID(ctx context.Context) context.Context {
	return nbs_client.WithClientID(ctx, s.clientID)
}
