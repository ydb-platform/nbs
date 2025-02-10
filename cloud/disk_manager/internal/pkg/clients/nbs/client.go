package nbs

import (
	"context"
	"fmt"
	"math/bits"
	"strings"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	private_protos "github.com/ydb-platform/nbs/cloud/blockstore/private/api/protos"
	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	core_protos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/tracing"
)

////////////////////////////////////////////////////////////////////////////////

const (
	maxConsecutiveRetries            = 3
	maxChangedBlockCountPerIteration = 1 << 20
	blockCountToSaveStateThreshold   = 20 * maxChangedBlockCountPerIteration
)

////////////////////////////////////////////////////////////////////////////////

func highestBitPosition(b byte) int {
	i := 0
	for ; b != 0; b = b >> 1 {
		i++
	}
	return i
}

////////////////////////////////////////////////////////////////////////////////

func setProtoFlag(currentFlags uint32, flag protos.EMountFlag) uint32 {
	return currentFlags | (1 << (flag - 1))
}

func protoFlags(flag protos.EMountFlag) uint32 {
	return setProtoFlag(0, flag)
}

////////////////////////////////////////////////////////////////////////////////

func getStorageMediaKind(
	diskKind types.DiskKind,
) (core_protos.EStorageMediaKind, error) {

	switch diskKind {
	case types.DiskKind_DISK_KIND_SSD:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD, nil
	case types.DiskKind_DISK_KIND_HDD:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_HYBRID, nil
	case types.DiskKind_DISK_KIND_SSD_NONREPLICATED:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED, nil
	case types.DiskKind_DISK_KIND_SSD_MIRROR2:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2, nil
	case types.DiskKind_DISK_KIND_SSD_MIRROR3:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3, nil
	case types.DiskKind_DISK_KIND_SSD_LOCAL:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL, nil
	case types.DiskKind_DISK_KIND_HDD_NONREPLICATED:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED, nil
	case types.DiskKind_DISK_KIND_HDD_LOCAL:
		return core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD_LOCAL, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown disk kind %v",
			diskKind,
		)
	}
}

func getDiskKind(
	mediaKind core_protos.EStorageMediaKind,
) (types.DiskKind, error) {

	switch mediaKind {
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_DEFAULT:
		return 0, errors.NewNonRetriableErrorf(
			"unsupported media kind %v",
			mediaKind,
		)
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD:
		return types.DiskKind_DISK_KIND_SSD, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_HYBRID:
		return types.DiskKind_DISK_KIND_HDD, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD:
		return types.DiskKind_DISK_KIND_HDD, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED:
		return types.DiskKind_DISK_KIND_SSD_NONREPLICATED, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2:
		return types.DiskKind_DISK_KIND_SSD_MIRROR2, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL:
		return types.DiskKind_DISK_KIND_SSD_LOCAL, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3:
		return types.DiskKind_DISK_KIND_SSD_MIRROR3, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED:
		return types.DiskKind_DISK_KIND_HDD_NONREPLICATED, nil
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD_LOCAL:
		return types.DiskKind_DISK_KIND_HDD_LOCAL, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown media kind %v",
			mediaKind,
		)
	}
}

////////////////////////////////////////////////////////////////////////////////

func IsDiskRegistryBasedDisk(kind types.DiskKind) bool {
	mediaKind, err := getStorageMediaKind(kind)
	if err != nil {
		return false
	}

	return isDiskRegistryBasedDisk(mediaKind)
}

func isDiskRegistryBasedDisk(mediaKind core_protos.EStorageMediaKind) bool {
	switch mediaKind {
	case core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_NONREPLICATED,
		core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR2,
		core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_MIRROR3,
		core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD_NONREPLICATED:
		return true
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

func toEncryptionMode(
	mode types.EncryptionMode,
) (protos.EEncryptionMode, error) {

	switch mode {
	case types.EncryptionMode_NO_ENCRYPTION:
		return protos.EEncryptionMode_NO_ENCRYPTION, nil
	case types.EncryptionMode_ENCRYPTION_AES_XTS:
		return protos.EEncryptionMode_ENCRYPTION_AES_XTS, nil
	case types.EncryptionMode_ENCRYPTION_AT_REST:
		return protos.EEncryptionMode_ENCRYPTION_AT_REST, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown encryption mode %v",
			mode,
		)
	}
}

func fromEncryptionMode(
	mode protos.EEncryptionMode,
) (types.EncryptionMode, error) {

	switch mode {
	case protos.EEncryptionMode_NO_ENCRYPTION:
		return types.EncryptionMode_NO_ENCRYPTION, nil
	case protos.EEncryptionMode_ENCRYPTION_AES_XTS:
		return types.EncryptionMode_ENCRYPTION_AES_XTS, nil
	case protos.EEncryptionMode_ENCRYPTION_AT_REST:
		return types.EncryptionMode_ENCRYPTION_AT_REST, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown encryption mode %v",
			mode,
		)
	}
}

func getEncryptionSpec(
	encryptionDesc *types.EncryptionDesc,
) (*protos.TEncryptionSpec, error) {

	if encryptionDesc == nil {
		return nil, nil
	}

	encryptionMode, err := toEncryptionMode(encryptionDesc.Mode)
	if err != nil {
		return nil, err
	}

	encryptionSpec := &protos.TEncryptionSpec{
		Mode: encryptionMode,
	}

	switch key := encryptionDesc.Key.(type) {
	case *types.EncryptionDesc_KeyHash:
		encryptionSpec.KeyParam = &protos.TEncryptionSpec_KeyHash{
			KeyHash: key.KeyHash,
		}
	case *types.EncryptionDesc_KmsKey:
		encryptionSpec.KeyParam = &protos.TEncryptionSpec_KeyPath{
			KeyPath: &protos.TKeyPath{
				Path: &protos.TKeyPath_KmsKey{
					KmsKey: &protos.TKmsKey{
						KekId:        key.KmsKey.KekId,
						EncryptedDEK: key.KmsKey.EncryptedDEK,
						TaskId:       key.KmsKey.TaskId,
					},
				},
			},
		}
	case nil:
		encryptionSpec.KeyParam = nil
	default:
		return nil, errors.NewNonRetriableErrorf("unknown key %s", key)
	}

	return encryptionSpec, nil
}

func getEncryptionDesc(
	encryptionDesc *protos.TEncryptionDesc,
) (*types.EncryptionDesc, error) {

	if encryptionDesc == nil {
		return nil, nil
	}

	encryptionMode, err := fromEncryptionMode(encryptionDesc.Mode)
	if err != nil {
		return nil, err
	}

	if encryptionDesc.EncryptionKey != nil && len(encryptionDesc.KeyHash) != 0 {
		return nil, errors.NewNonRetriableErrorf(
			"ill-formed EncryptionDesc: EncryptionKey and KeyHash cannot be " +
				"provided simultaneously")
	}

	var resultDesc *types.EncryptionDesc

	if encryptionDesc.EncryptionKey != nil {
		resultDesc = &types.EncryptionDesc{
			Mode: encryptionMode,
			Key: &types.EncryptionDesc_KmsKey{
				KmsKey: &types.KmsKey{
					KekId:        encryptionDesc.EncryptionKey.KekId,
					EncryptedDEK: encryptionDesc.EncryptionKey.EncryptedDEK,
					TaskId:       encryptionDesc.EncryptionKey.TaskId,
				},
			},
		}
	} else if len(encryptionDesc.KeyHash) != 0 {
		resultDesc = &types.EncryptionDesc{
			Mode: encryptionMode,
			Key: &types.EncryptionDesc_KeyHash{
				KeyHash: encryptionDesc.KeyHash,
			},
		}
	} else {
		resultDesc = &types.EncryptionDesc{
			Mode: encryptionMode,
		}
	}

	return resultDesc, nil
}

func toPlacementStrategy(
	placementStrategy types.PlacementStrategy,
) (protos.EPlacementStrategy, error) {

	switch placementStrategy {
	case types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD:
		return protos.EPlacementStrategy_PLACEMENT_STRATEGY_SPREAD, nil
	case types.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION:
		return protos.EPlacementStrategy_PLACEMENT_STRATEGY_PARTITION, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown placement strategy %v",
			placementStrategy,
		)
	}
}

func fromPlacementStrategy(
	placementStrategy protos.EPlacementStrategy,
) (types.PlacementStrategy, error) {

	switch placementStrategy {
	case protos.EPlacementStrategy_PLACEMENT_STRATEGY_SPREAD:
		return types.PlacementStrategy_PLACEMENT_STRATEGY_SPREAD, nil
	case protos.EPlacementStrategy_PLACEMENT_STRATEGY_PARTITION:
		return types.PlacementStrategy_PLACEMENT_STRATEGY_PARTITION, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown placement strategy %v",
			placementStrategy,
		)
	}
}

func fromLogoBlobID(logoBlobID *private_protos.TLogoBlobID) string {
	return fmt.Sprintf(
		"RawX1:%v_RawX2:%v_RawX3:%v",
		logoBlobID.RawX1,
		logoBlobID.RawX2,
		logoBlobID.RawX3,
	)
}

func fromScanDiskProgress(
	scanDiskProgress *private_protos.TScanDiskProgress,
) ScanDiskStatus {

	if scanDiskProgress == nil {
		return ScanDiskStatus{}
	}

	var brokenBlobs []string
	for _, blobID := range scanDiskProgress.BrokenBlobs {
		brokenBlobs = append(brokenBlobs, fromLogoBlobID(blobID))
	}
	return ScanDiskStatus{
		Processed:   scanDiskProgress.Processed,
		Total:       scanDiskProgress.Total,
		IsCompleted: scanDiskProgress.IsCompleted,
		BrokenBlobs: brokenBlobs,
	}
}

////////////////////////////////////////////////////////////////////////////////

func wrapError(e error) error {
	return wrapErrorWithInternalFlag(e, true)
}

func wrapErrorWithInternalFlag(e error, internal bool) error {
	if IsNotFoundError(e) {
		return errors.NewSilentNonRetriableError(e)
	}

	var clientErr *nbs_client.ClientError
	if errors.As(e, &clientErr) {
		if clientErr.IsRetriable() ||
			clientErr.Code == nbs_client.E_CANCELLED ||
			clientErr.Code == nbs_client.E_INVALID_SESSION ||
			clientErr.Code == nbs_client.E_MOUNT_CONFLICT {

			return errors.NewRetriableError(e)
		}

		switch clientErr.Code {
		case nbs_client.E_PRECONDITION_FAILED:
			e = errors.NewDetailedError(
				e,
				&errors.ErrorDetails{
					Code:     codes.PreconditionFailed,
					Message:  clientErr.Message,
					Internal: internal,
				},
			)
		case nbs_client.E_RESOURCE_EXHAUSTED:
			e = errors.NewDetailedError(
				e,
				&errors.ErrorDetails{
					Code:     codes.ResourceExhausted,
					Message:  clientErr.Message,
					Internal: internal,
				},
			)
		}
	}

	return e
}

func isAbortedError(e error) bool {
	var clientErr *nbs_client.ClientError
	if errors.As(e, &clientErr) {
		if clientErr.Code == nbs_client.E_ABORTED {
			return true
		}
	}

	return false
}

func IsDiskNotFoundError(e error) bool {
	return nbs_client.IsDiskNotFoundError(e)
}

func IsNotFoundError(e error) bool {
	if IsDiskNotFoundError(e) {
		return true
	}

	var clientErr *nbs_client.ClientError
	return errors.As(e, &clientErr) && clientErr.Code == nbs_client.E_NOT_FOUND
}

func IsGetChangedBlocksNotSupportedError(e error) bool {
	clientErr := nbs_client.GetClientError(e)

	// TODO: don't check E_ARGUMENT after https://github.com/ydb-platform/nbs/issues/1297#issuecomment-2149816298
	return clientErr.Code == nbs_client.E_ARGUMENT && strings.Contains(clientErr.Error(), "Disk registry based disks can not handle GetChangedBlocks requests for normal checkpoints") ||
		clientErr.Code == nbs_client.E_NOT_IMPLEMENTED
}

func IsAlterPlacementGroupMembershipPublicError(e error) bool {
	clientErr := nbs_client.GetClientError(e)

	if clientErr.Code == nbs_client.E_RESOURCE_EXHAUSTED &&
		strings.Contains(clientErr.Message, "max disk count in group exceeded") {
		return true
	}

	if clientErr.Code == nbs_client.E_PRECONDITION_FAILED &&
		strings.Contains(clientErr.Message, "failed to add some disks") {
		return true
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

func setupStderrLogger(ctx context.Context) context.Context {
	return logging.SetLogger(
		ctx,
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func min(x, y uint64) uint64 {
	if x > y {
		return y
	}

	return x
}

////////////////////////////////////////////////////////////////////////////////

func areOverlayDisksSupported(mediaKind core_protos.EStorageMediaKind) bool {
	return mediaKind == core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD ||
		mediaKind == core_protos.EStorageMediaKind_STORAGE_MEDIA_HYBRID ||
		mediaKind == core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD
}

func canBeBaseDisk(volume *protos.TVolume) bool {
	return areOverlayDisksSupported(volume.StorageMediaKind) &&
		volume.EncryptionDesc.Mode == protos.EEncryptionMode_NO_ENCRYPTION && // NBS-3297
		len(volume.BaseDiskId) == 0 // overlay disk can't be used as base disk
}

////////////////////////////////////////////////////////////////////////////////

type client struct {
	nbs                           *nbs_client.DiscoveryClient
	sessionMetricsRegistry        metrics.Registry
	metrics                       *clientMetrics
	enableThrottlingForMediaKinds []core_protos.EStorageMediaKind
	sessionRediscoverPeriodMin    time.Duration
	sessionRediscoverPeriodMax    time.Duration
	serverRequestTimeout          time.Duration
}

func (c *client) updateVolume(
	ctx context.Context,
	saveState func() error,
	diskID string,
	do func(volume *protos.TVolume) error,
) (err error) {

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.updateVolume",
		tracing.WithAttributes(tracing.AttributeString("disk_id", diskID)),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	retries := 0
	for {
		volume, err := c.describeVolume(ctx, diskID)
		if err != nil {
			return err
		}

		err = saveState()
		if err != nil {
			return err
		}

		err = do(volume)
		if err != nil {
			if !isAbortedError(err) {
				return err
			}

			if retries == maxConsecutiveRetries {
				return errors.NewRetriableError(err)
			}

			retries++
			span.AddEvent(
				"Retry in blockstore client",
				tracing.WithAttributes(
					tracing.AttributeInt("failed_attempts", retries),
					tracing.AttributeError(err),
				),
			)
			continue
		}

		return nil
	}
}

// TODO: unify with updateVolume.
func (c *client) updatePlacementGroup(
	ctx context.Context,
	saveState func() error,
	groupID string,
	do func(group *protos.TPlacementGroup) error,
) (err error) {

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.updatePlacementGroup",
		tracing.WithAttributes(tracing.AttributeString("group_id", groupID)),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	retries := 0
	for {
		group, err := c.describePlacementGroup(ctx, groupID)
		if err != nil {
			return err
		}

		err = saveState()
		if err != nil {
			return err
		}

		err = do(group)
		if err != nil {
			if !isAbortedError(err) {
				return err
			}

			if retries == maxConsecutiveRetries {
				return errors.NewRetriableError(err)
			}

			retries++
			span.AddEvent(
				"Retry in blockstore client",
				tracing.WithAttributes(
					tracing.AttributeInt("failed_attempts", retries),
					tracing.AttributeError(err),
				),
			)
			continue
		}

		return nil
	}
}

func (c *client) executeAction(
	ctx context.Context,
	action string,
	request proto.Message,
	response proto.Message,
) (err error) {

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.executeAction",
		tracing.WithAttributes(
			tracing.AttributeString("action", action),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	input, err := new(jsonpb.Marshaler).MarshalToString(request)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"failed to marshal request: %v",
			err,
		)
	}

	span.SetAttributes(tracing.AttributeString("input", input))

	ctx = c.withTimeoutHeader(ctx)
	output, err := c.nbs.ExecuteAction(ctx, action, []byte(input))
	if err != nil {
		return wrapError(err)
	}

	outputString := string(output)
	span.SetAttributes(tracing.AttributeString("output", outputString))
	err = new(jsonpb.Unmarshaler).Unmarshal(
		strings.NewReader(outputString),
		response,
	)
	if err != nil {
		return errors.NewNonRetriableErrorf(
			"failed to unmarshal response: %v",
			err,
		)
	}

	return nil
}

func (c *client) getMountFlags(
	ctx context.Context,
	diskID string,
) (uint32, error) {

	if len(c.enableThrottlingForMediaKinds) != 0 {
		volume, err := c.describeVolume(ctx, diskID)
		if err != nil {
			return 0, err
		}

		for _, throttledMediaKind := range c.enableThrottlingForMediaKinds {
			if throttledMediaKind == volume.StorageMediaKind {
				return protoFlags(protos.EMountFlag_MF_NONE), nil
			}
		}
	}

	return protoFlags(protos.EMountFlag_MF_THROTTLING_DISABLED), nil
}

////////////////////////////////////////////////////////////////////////////////

func (t CheckpointType) toProto() protos.ECheckpointType {
	return map[CheckpointType]protos.ECheckpointType{
		CheckpointTypeNormal:      protos.ECheckpointType_NORMAL,
		CheckpointTypeLight:       protos.ECheckpointType_LIGHT,
		CheckpointTypeWithoutData: protos.ECheckpointType_WITHOUT_DATA,
	}[t]
}

func parseCheckpointStatus(protoType protos.ECheckpointStatus) CheckpointStatus {
	switch protoType {
	case protos.ECheckpointStatus_NOT_READY:
		return CheckpointStatusNotReady
	case protos.ECheckpointStatus_READY:
		return CheckpointStatusReady
	case protos.ECheckpointStatus_ERROR:
		return CheckpointStatusError
	default:
		return CheckpointStatusError
	}
}

func (m CheckpointStatus) String() string {
	switch m {
	case CheckpointStatusNotReady:
		return "CheckpointStatusNotReady"
	case CheckpointStatusReady:
		return "CheckpointStatusReady"
	case CheckpointStatusError:
		return "CheckpointStatusError"
	default:
		return "CheckpointStatusUnknown"
	}
}

////////////////////////////////////////////////////////////////////////////////

func (c *client) Ping(ctx context.Context) (err error) {
	defer c.metrics.StatRequest("Ping")(&err)
	return c.ping(ctx)
}

func (c *client) Create(
	ctx context.Context,
	params CreateDiskParams,
) (err error) {

	defer c.metrics.StatRequest("Create")(&err)

	kind, err := getStorageMediaKind(params.Kind)
	if err != nil {
		return err
	}

	encryptionSpec, err := getEncryptionSpec(params.EncryptionDesc)
	if err != nil {
		return err
	}

	return c.createVolume(
		ctx,
		params.ID,
		params.BlocksCount,
		&nbs_client.CreateVolumeOpts{
			BaseDiskId:              params.BaseDiskID,
			BaseDiskCheckpointId:    params.BaseDiskCheckpointID,
			BlockSize:               params.BlockSize,
			StorageMediaKind:        kind,
			CloudId:                 params.CloudID,
			FolderId:                params.FolderID,
			TabletVersion:           params.TabletVersion,
			PlacementGroupId:        params.PlacementGroupID,
			PlacementPartitionIndex: params.PlacementPartitionIndex,
			PartitionsCount:         params.PartitionsCount,
			IsSystem:                params.IsSystem,
			StoragePoolName:         params.StoragePoolName,
			AgentIds:                params.AgentIds,
			EncryptionSpec:          encryptionSpec,
		},
	)
}

func (c *client) CreateProxyOverlayDisk(
	ctx context.Context,
	diskID string,
	baseDiskID string,
	baseDiskCheckpointID string,
) (created bool, err error) {

	defer c.metrics.StatRequest("CreateProxyOverlayDisk")(&err)

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.CreateProxyOverlayDisk",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("base_disk_id", baseDiskID),
			tracing.AttributeString(
				"base_disk_checkpoint_id",
				baseDiskCheckpointID,
			),
		),
	)
	defer span.End()
	defer func(created *bool) {
		span.SetAttributes(tracing.AttributeBool("created", *created))
	}(&created)

	volume, err := c.describeVolume(ctx, baseDiskID)
	if err != nil {
		return false, err
	}

	if !canBeBaseDisk(volume) {
		return false, nil
	}

	err = c.createVolume(
		ctx,
		diskID,
		volume.BlocksCount,
		&nbs_client.CreateVolumeOpts{
			BaseDiskId:           baseDiskID,
			BaseDiskCheckpointId: baseDiskCheckpointID,
			BlockSize:            volume.BlockSize,
			StorageMediaKind:     core_protos.EStorageMediaKind_STORAGE_MEDIA_HYBRID,
			PartitionsCount:      1,
			IsSystem:             true,
			EncryptionSpec: &protos.TEncryptionSpec{
				Mode: volume.EncryptionDesc.Mode,
				KeyParam: &protos.TEncryptionSpec_KeyHash{
					KeyHash: volume.EncryptionDesc.KeyHash,
				},
			},
		},
	)
	if err != nil {
		return false, err
	}

	err = c.createCheckpoint(
		ctx,
		diskID,
		baseDiskCheckpointID,
		CheckpointTypeNormal.toProto(),
	)
	if err != nil {
		return false, err
	}

	return true, nil
}

func (c *client) Delete(
	ctx context.Context,
	diskID string,
) (err error) {

	defer c.metrics.StatRequest("Delete")(&err)

	return c.destroyVolume(
		ctx,
		diskID,
		false, // sync
		0,     // fillGeneration
	)
}

func (c *client) DeleteSync(
	ctx context.Context,
	diskID string,
) (err error) {

	defer c.metrics.StatRequest("DeleteSync")(&err)

	return c.destroyVolume(
		ctx,
		diskID,
		true, // sync
		0,    // fillGeneration
	)
}

func (c *client) DeleteWithFillGeneration(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
) (err error) {

	defer c.metrics.StatRequest("DeleteWithFillGeneration")(&err)

	return c.destroyVolume(
		ctx,
		diskID,
		false, // sync
		fillGeneration,
	)
}

func (c *client) CreateCheckpoint(
	ctx context.Context,
	params CheckpointParams,
) (err error) {

	defer c.metrics.StatRequest("CreateCheckpoint")(&err)

	return c.createCheckpoint(
		ctx,
		params.DiskID,
		params.CheckpointID,
		params.CheckpointType.toProto(),
	)
}

func (c *client) GetCheckpointStatus(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (CheckpointStatus, error) {

	status, err := c.getCheckpointStatus(ctx, diskID, checkpointID)
	return parseCheckpointStatus(status), err
}

func (c *client) DeleteCheckpoint(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (err error) {

	defer c.metrics.StatRequest("DeleteCheckpoint")(&err)

	err = c.deleteCheckpoint(ctx, diskID, checkpointID)
	if IsNotFoundError(err) {
		return nil
	}
	return err
}

func (c *client) DeleteCheckpointData(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (err error) {

	defer c.metrics.StatRequest("DeleteCheckpointData")(&err)

	response := &private_protos.TDeleteCheckpointDataResponse{}

	err = c.executeAction(
		ctx,
		"DeleteCheckpointData",
		&private_protos.TDeleteCheckpointDataRequest{
			DiskId:       diskID,
			CheckpointId: checkpointID,
		},
		response,
	)
	if IsNotFoundError(err) {
		return nil
	}

	return err
}

func (c *client) EnsureCheckpointReady(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (err error) {

	defer c.metrics.StatRequest("EnsureCheckpointReady")(&err)

	status, err := c.GetCheckpointStatus(ctx, diskID, checkpointID)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"Current status of checkpoint with id %v for disk %v is %v",
		checkpointID,
		diskID,
		status,
	)

	switch status {
	case CheckpointStatusNotReady:
		return errors.NewInterruptExecutionError()

	case CheckpointStatusError:
		return errors.NewRetriableErrorf(
			"creating checkpoint with id %v for disk %v ended with an error", // TODO:_ why creating? Maybe filling?
			checkpointID,
			diskID,
		)

	case CheckpointStatusReady:
		// Nothing to do.
	}

	return nil
}

func (c *client) Resize(
	ctx context.Context,
	checkpoint func() error,
	diskID string,
	size uint64,
) (err error) {

	defer c.metrics.StatRequest("Resize")(&err)

	return c.updateVolume(ctx, checkpoint, diskID, func(volume *protos.TVolume) error {
		if volume.BlockSize == 0 {
			return errors.NewNonRetriableErrorf(
				"invalid volume config %v",
				volume,
			)
		}

		if size%uint64(volume.BlockSize) != 0 {
			return errors.NewNonRetriableErrorf(
				"size %v should be divisible by volume.BlockSize %v",
				size,
				volume.BlockSize,
			)
		}
		newBlocksCount := size / uint64(volume.BlockSize)

		return c.resizeVolume(
			ctx,
			diskID,
			newBlocksCount,
			0, // channelsCount
			volume.ConfigVersion,
		)
	})
}

func (c *client) Alter(
	ctx context.Context,
	saveState func() error,
	diskID string,
	cloudID string,
	folderID string,
) (err error) {

	defer c.metrics.StatRequest("Alter")(&err)

	return c.updateVolume(ctx, saveState, diskID, func(volume *protos.TVolume) error {
		return c.alterVolume(
			ctx,
			diskID,
			volume.ProjectId,
			folderID,
			cloudID,
			volume.ConfigVersion,
		)
	})
}

func (c *client) Rebase(
	ctx context.Context,
	saveState func() error,
	diskID string,
	baseDiskID string,
	targetBaseDiskID string,
) (err error) {

	defer c.metrics.StatRequest("Rebase")(&err)

	return c.updateVolume(ctx, saveState, diskID, func(volume *protos.TVolume) error {
		if volume.BaseDiskId == targetBaseDiskID {
			// Should be idempotent.
			return nil
		}

		if volume.BaseDiskId != baseDiskID {
			return errors.NewNonRetriableErrorf(
				"unexpected baseDiskID for rebase, expected=%v, actual=%v",
				baseDiskID,
				volume.BaseDiskId,
			)
		}

		response := &private_protos.TRebaseVolumeResponse{}
		return c.executeAction(
			ctx,
			"RebaseVolume",
			&private_protos.TRebaseVolumeRequest{
				DiskId:           diskID,
				TargetBaseDiskId: targetBaseDiskID,
				ConfigVersion:    volume.ConfigVersion,
			},
			response,
		)
	})
}

func (c *client) Assign(
	ctx context.Context,
	params AssignDiskParams,
) (err error) {

	defer c.metrics.StatRequest("Assign")(&err)

	_, err = c.assignVolume(
		ctx,
		params.ID,
		params.InstanceID,
		params.Token,
		params.Host,
	)
	return err
}

func (c *client) Unassign(
	ctx context.Context,
	diskID string,
) (err error) {

	defer c.metrics.StatRequest("Unassign")(&err)

	_, err = c.assignVolume(
		ctx,
		diskID,
		"",
		"",
		"",
	)
	if IsNotFoundError(err) {
		return nil
	}
	return err
}

func (c *client) DescribeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind types.DiskKind,
	tabletVersion uint32,
) (diskModel DiskModel, err error) {

	defer c.metrics.StatRequest("DescribeModel")(&err)

	mediaKind, err := getStorageMediaKind(kind)
	if err != nil {
		return DiskModel{}, err
	}

	model, err := c.describeVolumeModel(
		ctx,
		blocksCount,
		blockSize,
		mediaKind,
		tabletVersion,
	)
	if err != nil {
		return DiskModel{}, err
	}

	return DiskModel{
		BlockSize:     model.BlockSize,
		BlocksCount:   model.BlocksCount,
		ChannelsCount: model.ChannelsCount,
		Kind:          kind,
		PerformanceProfile: DiskPerformanceProfile{
			MaxReadBandwidth:   model.PerformanceProfile.MaxReadBandwidth,
			MaxPostponedWeight: model.PerformanceProfile.MaxPostponedWeight,
			ThrottlingEnabled:  model.PerformanceProfile.ThrottlingEnabled,
			MaxReadIops:        model.PerformanceProfile.MaxReadIops,
			BoostTime:          model.PerformanceProfile.BoostTime,
			BoostRefillTime:    model.PerformanceProfile.BoostRefillTime,
			BoostPercentage:    model.PerformanceProfile.BoostPercentage,
			MaxWriteBandwidth:  model.PerformanceProfile.MaxWriteBandwidth,
			MaxWriteIops:       model.PerformanceProfile.MaxWriteIops,
			BurstPercentage:    model.PerformanceProfile.BurstPercentage,
		},
		MergedChannelsCount: model.MergedChannelsCount,
		MixedChannelsCount:  model.MixedChannelsCount,
	}, nil
}

func (c *client) Describe(
	ctx context.Context,
	diskID string,
) (diskParams DiskParams, err error) {

	defer c.metrics.StatRequest("Describe")(&err)

	volume, err := c.describeVolume(ctx, diskID)
	if err != nil {
		return DiskParams{}, err
	}

	encryptionDesc, err := getEncryptionDesc(volume.EncryptionDesc)
	if err != nil {
		return DiskParams{}, err
	}

	diskKind, err := getDiskKind(volume.StorageMediaKind)
	if err != nil {
		return DiskParams{}, err
	}

	return DiskParams{
		BlockSize:      volume.BlockSize,
		BlocksCount:    volume.BlocksCount,
		Kind:           diskKind,
		EncryptionDesc: encryptionDesc,
		CloudID:        volume.CloudId,
		FolderID:       volume.FolderId,
		BaseDiskID:     volume.BaseDiskId,
		IsFillFinished: volume.IsFillFinished,

		IsDiskRegistryBasedDisk: isDiskRegistryBasedDisk(volume.StorageMediaKind),
	}, nil
}

func (c *client) CreatePlacementGroup(
	ctx context.Context,
	groupID string,
	placementStrategy types.PlacementStrategy,
	placementPartitionCount uint32,
) (err error) {

	defer c.metrics.StatRequest("CreatePlacementGroup")(&err)

	strategy, err := toPlacementStrategy(placementStrategy)
	if err != nil {
		return err
	}

	return c.createPlacementGroup(ctx, groupID, strategy, placementPartitionCount)
}

func (c *client) DeletePlacementGroup(
	ctx context.Context,
	groupID string,
) (err error) {

	defer c.metrics.StatRequest("DeletePlacementGroup")(&err)
	return c.destroyPlacementGroup(ctx, groupID)
}

func (c *client) AlterPlacementGroupMembership(
	ctx context.Context,
	saveState func() error,
	groupID string,
	placementPartitionIndex uint32,
	disksToAdd []string,
	disksToRemove []string,
) (err error) {

	defer c.metrics.StatRequest("AlterPlacementGroupMembership")(&err)

	return c.updatePlacementGroup(ctx, saveState, groupID, func(group *protos.TPlacementGroup) error {
		return c.alterPlacementGroupMembership(
			ctx,
			groupID,
			placementPartitionIndex,
			disksToAdd,
			disksToRemove,
			group.ConfigVersion,
		)
	})
}

func (c *client) ListPlacementGroups(
	ctx context.Context,
) (groups []string, err error) {

	defer c.metrics.StatRequest("ListPlacementGroups")(&err)

	groups, err = c.listPlacementGroups(ctx)
	if err != nil {
		return nil, err
	}

	return groups, nil
}

func (c *client) DescribePlacementGroup(
	ctx context.Context,
	groupID string,
) (placementGroup PlacementGroup, err error) {

	defer c.metrics.StatRequest("DescribePlacementGroup")(&err)

	group, err := c.describePlacementGroup(ctx, groupID)
	if err != nil {
		return PlacementGroup{}, err
	}

	strategy, err := fromPlacementStrategy(group.PlacementStrategy)
	if err != nil {
		return PlacementGroup{}, err
	}

	return PlacementGroup{
		GroupID:           group.GroupId,
		PlacementStrategy: strategy,
		DiskIDs:           group.DiskIds,
		Racks:             group.Racks,
	}, nil
}

func (c *client) MountRO(
	ctx context.Context,
	diskID string,
	encryption *types.EncryptionDesc,
) (session *Session, err error) {

	defer c.metrics.StatRequest("MountRO")(&err)

	mountFlags, err := c.getMountFlags(ctx, diskID)
	if err != nil {
		return nil, err
	}

	encryptionSpec, err := getEncryptionSpec(encryption)
	if err != nil {
		return nil, err
	}

	return NewROSession(
		ctx,
		c.nbs,
		c.sessionMetricsRegistry,
		diskID,
		mountFlags,
		encryptionSpec,
		c.sessionRediscoverPeriodMin,
		c.sessionRediscoverPeriodMax,
	)
}

func (c *client) MountLocalRO(
	ctx context.Context,
	diskID string,
	encryption *types.EncryptionDesc,
) (session *Session, err error) {

	defer c.metrics.StatRequest("MountLocalRO")(&err)

	mountFlags, err := c.getMountFlags(ctx, diskID)
	if err != nil {
		return nil, err
	}

	encryptionSpec, err := getEncryptionSpec(encryption)
	if err != nil {
		return nil, err
	}

	return NewLocalROSession(
		ctx,
		c.nbs,
		c.sessionMetricsRegistry,
		diskID,
		mountFlags,
		encryptionSpec,
		c.sessionRediscoverPeriodMin,
		c.sessionRediscoverPeriodMax,
	)
}

func (c *client) MountRW(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
	fillSeqNumber uint64,
	encryption *types.EncryptionDesc,
) (session *Session, err error) {

	defer c.metrics.StatRequest("MountRW")(&err)

	mountFlags, err := c.getMountFlags(ctx, diskID)
	if err != nil {
		return nil, err
	}

	encryptionSpec, err := getEncryptionSpec(encryption)
	if err != nil {
		return nil, err
	}

	return NewRWSession(
		ctx,
		c.nbs,
		c.sessionMetricsRegistry,
		diskID,
		fillGeneration,
		fillSeqNumber,
		mountFlags,
		encryptionSpec,
		c.sessionRediscoverPeriodMin,
		c.sessionRediscoverPeriodMax,
	)
}

func (c *client) GetChangedBlocks(
	ctx context.Context,
	diskID string,
	startIndex uint64,
	blockCount uint32,
	baseCheckpointID,
	checkpointID string,
	ignoreBaseDisk bool,
) (blockMask []byte, err error) {

	defer c.metrics.StatRequest("GetChangedBlocks")(&err)

	return c.getChangedBlocks(
		ctx,
		diskID,
		startIndex,
		blockCount,
		baseCheckpointID, // lowCheckpointID
		checkpointID,     // highCheckpointID
		ignoreBaseDisk,
	)
}

func (c *client) GetCheckpointSize(
	ctx context.Context,
	saveState func(blockIndex uint64, checkpointSize uint64) error,
	diskID string,
	checkpointID string,
	milestoneBlockIndex uint64,
	milestoneCheckpointSize uint64,
) (err error) {

	defer c.metrics.StatRequest("GetCheckpointSize")(&err)

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.GetCheckpointSize",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("checkpoint_id", checkpointID),
			tracing.AttributeInt64(
				"milestone_block_index",
				int64(milestoneBlockIndex),
			),
			tracing.AttributeInt64(
				"milestone_checkpoint_size",
				int64(milestoneCheckpointSize),
			),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	volume, err := c.describeVolume(ctx, diskID)
	if err != nil {
		return err
	}

	blockIndex := milestoneBlockIndex
	if blockIndex >= volume.BlocksCount {
		return nil
	}

	maxUsedBlockIndex := milestoneCheckpointSize / uint64(volume.BlockSize)

	for {
		blockCount := uint32(maxChangedBlockCountPerIteration)
		if uint64(blockCount) > volume.BlocksCount-blockIndex {
			blockCount = uint32(volume.BlocksCount - blockIndex)
		}

		blockMask, err := c.GetChangedBlocks(
			ctx,
			diskID,
			blockIndex,
			blockCount,
			"",
			checkpointID,
			false, // ignoreBaseDisk
		)
		if err != nil {
			return err
		}

		if len(blockMask) != 0 {
			for i := len(blockMask) - 1; i >= 0; i-- {
				h := highestBitPosition(blockMask[i])
				if h == 0 {
					continue
				}

				maxUsedBlockIndex = blockIndex + uint64(8*i) + uint64(h-1)
				break
			}
		}

		blockIndex += maxChangedBlockCountPerIteration
		checkpointSize := maxUsedBlockIndex * uint64(volume.BlockSize)

		if blockIndex >= volume.BlocksCount {
			return saveState(blockIndex, checkpointSize)
		}

		if blockIndex%(blockCountToSaveStateThreshold) == 0 {
			err = saveState(blockIndex, checkpointSize)
			if err != nil {
				return err
			}
		}
	}
}

func (c *client) GetChangedBytes(
	ctx context.Context,
	diskID string,
	baseCheckpointID string,
	checkpointID string,
	ignoreBaseDisk bool,
) (diff uint64, err error) {

	defer c.metrics.StatRequest("GetChangedBytes")(&err)

	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.GetChangedBytes",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("base_checkpoint_id", baseCheckpointID),
			tracing.AttributeString("checkpoint_id", checkpointID),
			tracing.AttributeBool("ignore_base_disk", ignoreBaseDisk),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)
	defer func(diff *uint64) {
		span.SetAttributes(tracing.AttributeInt64("diff", int64(*diff)))
	}(&diff)

	volume, err := c.describeVolume(ctx, diskID)
	if err != nil {
		return 0, err
	}

	for blockIndex := uint64(0); blockIndex < volume.BlocksCount; blockIndex += maxChangedBlockCountPerIteration {
		blockCount := uint32(maxChangedBlockCountPerIteration)
		if uint64(blockCount) > volume.BlocksCount-blockIndex {
			blockCount = uint32(volume.BlocksCount - blockIndex)
		}

		blockMask, err := c.GetChangedBlocks(
			ctx,
			diskID,
			blockIndex,
			blockCount,
			baseCheckpointID,
			checkpointID,
			ignoreBaseDisk,
		)
		if err != nil {
			return 0, err
		}

		// TODO: read 64 bits at once instead of 8 bits.
		for _, mask := range blockMask {
			diff += uint64(bits.OnesCount8(mask))
		}

		logging.Debug(
			ctx,
			"GetChangedBlocks diff for "+
				"diskID %v, "+
				"startIndex %v, "+
				"blockCount %v, "+
				"baseCheckpointID %v, "+
				"checkpointID %v, "+
				"ignoreBaseDisk %v, "+
				"diff %v",
			diskID,
			blockIndex,
			blockCount,
			baseCheckpointID,
			checkpointID,
			ignoreBaseDisk,
			diff,
		)
	}

	return diff * uint64(volume.BlockSize), nil
}

func (c *client) Stat(
	ctx context.Context,
	diskID string,
) (stats DiskStats, err error) {

	defer c.metrics.StatRequest("Stat")(&err)

	volume, volumeStats, err := c.statVolume(ctx, diskID, uint32(0))
	if err != nil {
		return DiskStats{}, err
	}

	return DiskStats{
		StorageSize: uint64(volume.BlockSize) * volumeStats.LogicalUsedBlocksCount,
	}, nil
}

func (c *client) Freeze(
	ctx context.Context,
	saveState func() error,
	diskID string,
) (err error) {

	defer c.metrics.StatRequest("Freeze")(&err)

	return c.updateVolume(ctx, saveState, diskID, func(volume *protos.TVolume) error {
		response := &private_protos.TModifyTagsResponse{}
		return c.executeAction(
			ctx,
			"ModifyTags",
			&private_protos.TModifyTagsRequest{
				DiskId:        diskID,
				TagsToAdd:     []string{"read-only"},
				TagsToRemove:  []string{},
				ConfigVersion: volume.ConfigVersion,
			},
			response,
		)
	})
}

func (c *client) Unfreeze(
	ctx context.Context,
	saveState func() error,
	diskID string,
) (err error) {

	defer c.metrics.StatRequest("Unfreeze")(&err)

	return c.updateVolume(ctx, saveState, diskID, func(volume *protos.TVolume) error {
		response := &private_protos.TModifyTagsResponse{}
		return c.executeAction(
			ctx,
			"ModifyTags",
			&private_protos.TModifyTagsRequest{
				DiskId:        diskID,
				TagsToAdd:     []string{},
				TagsToRemove:  []string{"read-only"},
				ConfigVersion: volume.ConfigVersion,
			},
			response,
		)
	})
}

func (c *client) ScanDisk(
	ctx context.Context,
	diskID string,
	batchSize uint32,
) (err error) {

	defer c.metrics.StatRequest("ScanDisk")(&err)

	response := &private_protos.TScanDiskResponse{}

	return c.executeAction(
		ctx,
		"ScanDisk",
		&private_protos.TScanDiskRequest{
			DiskId:    diskID,
			BatchSize: batchSize,
		},
		response,
	)
}

func (c *client) GetScanDiskStatus(
	ctx context.Context,
	diskID string,
) (progress ScanDiskStatus, err error) {

	defer c.metrics.StatRequest("GetScanDiskStatus")(&err)

	response := &private_protos.TGetScanDiskStatusResponse{}

	err = c.executeAction(
		ctx,
		"GetScanDiskStatus",
		&private_protos.TGetScanDiskStatusRequest{
			DiskId: diskID,
		},
		response,
	)
	if err != nil {
		return ScanDiskStatus{}, err
	}

	return fromScanDiskProgress(response.Progress), err
}

func (c *client) FinishFillDisk(
	ctx context.Context,
	saveState func() error,
	diskID string,
	fillGeneration uint64,
) (err error) {

	defer c.metrics.StatRequest("FinishFillDisk")(&err)

	return c.updateVolume(ctx, saveState, diskID, func(volume *protos.TVolume) error {
		response := &private_protos.TFinishFillDiskResponse{}
		return c.executeAction(
			ctx,
			"FinishFillDisk",
			&private_protos.TFinishFillDiskRequest{
				DiskId:         diskID,
				ConfigVersion:  volume.ConfigVersion,
				FillGeneration: fillGeneration,
			},
			response,
		)
	})
}

////////////////////////////////////////////////////////////////////////////////

func (c *client) withTimeoutHeader(ctx context.Context) context.Context {
	//nolint:SA1029
	return context.WithValue(
		ctx,
		nbs_client.RequestTimeoutHeaderKey,
		c.serverRequestTimeout,
	)
}

////////////////////////////////////////////////////////////////////////////////

func (c *client) createVolume(
	ctx context.Context,
	diskID string,
	blocksCount uint64,
	opts *nbs_client.CreateVolumeOpts,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.createVolume",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeInt64("blocks_count", int64(blocksCount)),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.CreateVolume(ctx, diskID, blocksCount, opts)
	return wrapError(err)
}

func (c *client) describeVolume(
	ctx context.Context,
	diskID string,
) (volume *protos.TVolume, err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.describeVolume",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	volume, err = c.nbs.DescribeVolume(ctx, diskID)
	if err != nil {
		return nil, wrapError(err)
	}
	return volume, err
}

func (c *client) describePlacementGroup(
	ctx context.Context,
	groupID string,
) (group *protos.TPlacementGroup, err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.describePlacementGroup",
		tracing.WithAttributes(
			tracing.AttributeString("group_id", groupID),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	group, err = c.nbs.DescribePlacementGroup(ctx, groupID)
	if err != nil {
		return nil, wrapError(err)
	}
	return group, err
}

func (c *client) ping(ctx context.Context) (err error) {
	ctx = c.withTimeoutHeader(ctx)
	err = c.nbs.Ping(ctx)
	return wrapError(err)
}

func (c *client) createCheckpoint(
	ctx context.Context,
	diskID string,
	checkpointID string,
	checkpointType protos.ECheckpointType,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.createCheckpoint",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("checkpoint_id", checkpointID),
			tracing.AttributeString(
				"checkpoint_type",
				protos.ECheckpointType_name[int32(checkpointType)],
			),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.CreateCheckpoint(ctx, diskID, checkpointID, checkpointType)
	return wrapError(err)
}

func (c *client) destroyVolume(
	ctx context.Context,
	diskID string,
	sync bool,
	fillGeneration uint64,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.destroyVolume",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeBool("sync", sync),
			tracing.AttributeInt64("fill_generation", int64(fillGeneration)),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.DestroyVolume(ctx, diskID, sync, fillGeneration)
	return wrapError(err)
}

func (c *client) getCheckpointStatus(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (status protos.ECheckpointStatus, err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.getCheckpointStatus",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("checkpoint_id", checkpointID),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	status, err = c.nbs.GetCheckpointStatus(ctx, diskID, checkpointID)
	span.SetAttributes(
		tracing.AttributeString(
			"status",
			protos.ECheckpointStatus_name[int32(status)],
		),
	)
	return status, wrapError(err)
}

func (c *client) deleteCheckpoint(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.deleteCheckpoint",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("checkpoint_id", checkpointID),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.DeleteCheckpoint(ctx, diskID, checkpointID)
	return wrapError(err)
}

func (c *client) resizeVolume(
	ctx context.Context,
	diskID string,
	blocksCount uint64,
	channelsCount uint32,
	configVersion uint32,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.resizeVolume",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeInt64("blocks_count", int64(blocksCount)),
			tracing.AttributeInt("channels_count", int(channelsCount)),
			tracing.AttributeInt("config_version", int(configVersion)),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.ResizeVolume(
		ctx,
		diskID,
		blocksCount,
		channelsCount,
		configVersion,
	)
	return wrapError(err)
}

func (c *client) alterVolume(
	ctx context.Context,
	diskID string,
	projectID string,
	folderID string,
	cloudID string,
	configVersion uint32,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.alterVolume",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeInt("config_version", int(configVersion)),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.AlterVolume(
		ctx,
		diskID,
		projectID,
		folderID,
		cloudID,
		configVersion,
	)
	return wrapError(err)
}

func (c *client) assignVolume(
	ctx context.Context,
	diskID string,
	instanceID string,
	token string,
	host string,
) (volume *protos.TVolume, err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.assignVolume",
		tracing.WithAttributes(
			tracing.AttributeString("disk_id", diskID),
			tracing.AttributeString("instance_id", instanceID),
			tracing.AttributeString("host", host),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	volume, err = c.nbs.AssignVolume(
		ctx,
		diskID,
		instanceID,
		token,
		host,
	)
	if err != nil {
		return nil, wrapError(err)
	}
	return volume, err
}

func (c *client) describeVolumeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	storageMediaKind core_protos.EStorageMediaKind,
	tabletVersion uint32,
) (model *protos.TVolumeModel, err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.describeVolumeModel",
		tracing.WithAttributes(
			tracing.AttributeInt64("blocks_count", int64(blocksCount)),
			tracing.AttributeInt("block_size", int(blockSize)),
			tracing.AttributeString(
				"storage_media_kind",
				core_protos.EStorageMediaKind_name[int32(storageMediaKind)],
			),
			tracing.AttributeInt("tablet_version", int(tabletVersion)),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	model, err = c.nbs.DescribeVolumeModel(
		ctx,
		blocksCount,
		blockSize,
		storageMediaKind,
		tabletVersion,
	)
	if err != nil {
		return nil, wrapError(err)
	}
	return model, err
}

func (c *client) createPlacementGroup(
	ctx context.Context,
	groupID string,
	placementStrategy protos.EPlacementStrategy,
	placementPartitionCount uint32,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.createPlacementGroup",
		tracing.WithAttributes(
			tracing.AttributeString("group_id", groupID),
			tracing.AttributeString(
				"placement_strategy",
				protos.EPlacementStrategy_name[int32(placementStrategy)],
			),
			tracing.AttributeInt(
				"placement_partition_count",
				int(placementPartitionCount),
			),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.CreatePlacementGroup(
		ctx,
		groupID,
		placementStrategy,
		placementPartitionCount,
	)
	return wrapError(err)
}

func (c *client) destroyPlacementGroup(
	ctx context.Context,
	groupID string,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.destroyPlacementGroup",
		tracing.WithAttributes(
			tracing.AttributeString("group_id", groupID),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.DestroyPlacementGroup(ctx, groupID)
	return wrapError(err)
}

func (c *client) alterPlacementGroupMembership(
	ctx context.Context,
	groupID string,
	placementPartitionIndex uint32,
	disksToAdd []string,
	disksToRemove []string,
	configVersion uint32,
) (err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.alterPlacementGroupMembership",
		tracing.WithAttributes(
			tracing.AttributeString("group_id", groupID),
			tracing.AttributeInt(
				"placement_partition_index",
				int(placementPartitionIndex),
			),
			tracing.AttributeInt("config_version", int(configVersion)),
		),
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	err = c.nbs.AlterPlacementGroupMembership(
		ctx,
		groupID,
		placementPartitionIndex,
		disksToAdd,
		disksToRemove,
		configVersion,
	)

	return wrapErrorWithInternalFlag(
		err,
		!IsAlterPlacementGroupMembershipPublicError(err),
	)
}

func (c *client) listPlacementGroups(
	ctx context.Context,
) (groups []string, err error) {

	ctx = c.withTimeoutHeader(ctx)
	ctx, span := tracing.StartSpan(
		ctx,
		"blockstore.listPlacementGroups",
	)
	defer span.End()
	defer tracing.SetError(span, &err)

	groups, err = c.nbs.ListPlacementGroups(ctx)
	if err != nil {
		return nil, wrapError(err)
	}
	return groups, err
}

func (c *client) getChangedBlocks(
	ctx context.Context,
	diskID string,
	startIndex uint64,
	blocksCount uint32,
	lowCheckpointID string,
	highCheckpointID string,
	ignoreBaseDisk bool,
) (blockMask []byte, err error) {

	ctx = c.withTimeoutHeader(ctx)
	blockMask, err = c.nbs.GetChangedBlocks(
		ctx,
		diskID,
		startIndex,
		blocksCount,
		lowCheckpointID,
		highCheckpointID,
		ignoreBaseDisk,
	)
	if err != nil {
		return nil, wrapError(err)
	}
	return blockMask, err
}

func (c *client) statVolume(
	ctx context.Context,
	diskID string,
	flags uint32,
) (volume *protos.TVolume, stats *protos.TVolumeStats, err error) {

	ctx = c.withTimeoutHeader(ctx)
	volume, stats, err = c.nbs.StatVolume(ctx, diskID, flags)
	if err != nil {
		return nil, nil, wrapError(err)
	}
	return volume, stats, err
}
