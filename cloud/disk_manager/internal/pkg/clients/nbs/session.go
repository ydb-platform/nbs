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
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
	"go.opentelemetry.io/otel/attribute"
	tracing_codes "go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////

// TODO:_ move to config
const dataplaneSamplingProbability = 0.01
const rediscoverSamplingProbability = 0.1

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
) (*Session, error) {

	clientID, err := generateClientID()
	if err != nil {
		return nil, err
	}

	session := &Session{
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

// TODO:_ BlocksCount
func (s *Session) BlockCount() uint64 {
	return s.volume.BlocksCount
}

func (s *Session) IsOverlayDisk() bool {
	return len(s.volume.BaseDiskId) != 0
}

func (s *Session) IsDiskRegistryBasedDisk() bool {
	return isDiskRegistryBasedDisk(s.volume.StorageMediaKind)
}

func (s *Session) Read(
	ctx context.Context,
	startIndex uint64,
	blocksCount uint32,
	checkpointID string,
	data []byte,
	zero *bool,
) (err error) {

	ctx, span := tracing.StartSpanWithProbabilisticSampling(
		ctx,
		"NBS.Session.Read",
		dataplaneSamplingProbability,
		trace.WithAttributes(
			attribute.Int64("start_index", int64(startIndex)),
			attribute.Int("blocks_count", int(blocksCount)),
			attribute.String("checkpoint_id", checkpointID),
		),
	)
	defer span.End()
	defer func() {
		// TODO:_ what if error is S_ALREADY? (not here, but in similar cases)
		if err != nil {
			span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
		}
	}()

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.session == nil {
		return errors.NewRetriableErrorf("last rediscover was failed")
	}

	defer s.metrics.StatRequest("Read")(&err)

	blocks, err := s.session.ReadBlocks(
		s.withClientID(ctx),
		startIndex,
		blocksCount,
		checkpointID,
	)
	if err != nil {
		return wrapError(err)
	}

	if nbs_client.AllBlocksEmpty(blocks) {
		*zero = true
		return nil
	}

	return nbs_client.JoinBlocks(s.BlockSize(), blocksCount, blocks, data)
}

func (s *Session) Write(
	ctx context.Context,
	startIndex uint64,
	data []byte,
) (err error) {

	blocksCount := uint32(len(data)) / s.BlockSize()

	ctx, span := tracing.StartSpanWithProbabilisticSampling(
		ctx,
		"NBS.Session.Write",
		dataplaneSamplingProbability,
		trace.WithAttributes(
			attribute.Int64("start_index", int64(startIndex)), // TODO:_ integer types hell
			attribute.Int("blocks_count", int(blocksCount)),
		),
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
		}
	}()

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.session == nil {
		return errors.NewRetriableErrorf("last rediscover was failed")
	}

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
	blocksCount uint32,
) (err error) {

	ctx, span := tracing.StartSpanWithProbabilisticSampling(
		ctx,
		"NBS.Session.Zero",
		dataplaneSamplingProbability,
		trace.WithAttributes(
			attribute.Int64("start_index", int64(startIndex)),
			attribute.Int("blocks_count", int(blocksCount)),
		),
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
		}
	}()

	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.session == nil {
		return errors.NewRetriableErrorf("last rediscover was failed")
	}

	defer s.metrics.StatRequest("Zero")(&err)

	err = s.session.ZeroBlocks(
		s.withClientID(ctx),
		startIndex,
		blocksCount,
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
	client, host, err := s.discoverInstance(ctx)
	if err != nil {
		return nil, err
	}

	s.client = client
	s.session = nbs_client.NewSession(
		*client,
		NewNbsClientLog(nbs_client.LOG_DEBUG),
	)
	s.metrics = newSessionMetrics(s.metricsRegistry, host)

	err = s.mountVolume(ctx)
	if err != nil {
		return nil, err
	}

	return s.session.Volume(), nil
}

func (s *Session) rediscover(ctx context.Context) {
	ctx, span := tracing.StartSpanWithProbabilisticSampling(
		ctx,
		makeSpanNameSDK("NBS.Session.rediscover"),
		rediscoverSamplingProbability,
		trace.WithAttributes(
			attribute.String("disk_id", s.diskID),
			attribute.String("client_id", s.clientID),
		),
	)
	defer span.End()

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if ctx.Err() != nil {
		return
	}

	s.closeSession(ctx)

	_, err := s.discoverAndMount(ctx)
	if err != nil {
		logging.Warn(ctx, "rediscover failed: %v", err)
		span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
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

	_ = s.unmountVolume(ctx)
	s.session.Close()
	_ = s.client.Close()

	s.session = nil
	s.client = nil
}

func (s *Session) withClientID(ctx context.Context) context.Context {
	return nbs_client.WithClientID(ctx, s.clientID)
}

////////////////////////////////////////////////////////////////////////////////

//	client, host, err := s.nbs.DiscoverInstance(ctx)
//	if err != nil {
//		return nil, wrapError(err)
//	}

func (s *Session) discoverInstance(
	ctx context.Context,
) (*nbs_client.Client, string, error) {

	ctx, span := tracing.StartSpan(
		ctx,
		makeSpanNameSDK("DiscoverInstance"),
		makeSpanAttributesSDK()...,
	)
	defer span.End()

	client, host, err := s.nbs.DiscoverInstance(ctx)
	if err != nil {
		err = wrapError(err)
		span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
	}

	return client, host, err
}

////////////////////////////////////////////////////////////////////////////////

func (s *Session) mountVolume(ctx context.Context) (err error) {
	ctx, span := tracing.StartSpan(
		ctx,
		makeSpanNameSDK("MountVolume"),
		trace.WithAttributes(
			attribute.String("disk_id", s.diskID),
			attribute.String("client_id", s.clientID),
			attribute.String(
				"access_mode",
				protos.EVolumeAccessMode_name[int32(s.mountOpts.AccessMode)],
			),
			attribute.String(
				"mount_mode",
				protos.EVolumeAccessMode_name[int32(s.mountOpts.MountMode)],
			),
		),
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
		}
	}()

	err = s.session.MountVolume(
		s.withClientID(ctx),
		s.diskID,
		&s.mountOpts,
	)
	if err != nil {
		err = wrapError(err) // TODO:_ put wrapped error into span (in other places)
	}
	return
}

func (s *Session) unmountVolume(ctx context.Context) (err error) {
	ctx, span := tracing.StartSpan(
		ctx,
		makeSpanNameSDK("MountVolume"),
		trace.WithAttributes( // TODO:_ should we write access mode and mount mode?
			attribute.String("disk_id", s.diskID),
			attribute.String("client_id", s.clientID),
		),
	)
	defer span.End()
	defer func() {
		if err != nil {
			span.SetStatus(tracing_codes.Error, fmt.Sprintf("%v", err))
		}
	}()

	err = s.session.UnmountVolume(s.withClientID(ctx))
	if err != nil {
		err = wrapError(err) // TODO:_ should we wrap error for unmount?
	}
	return
}
