package nbs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	private_protos "github.com/ydb-platform/nbs/cloud/blockstore/private/api/protos"
	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	core_protos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type VolumeLink struct {
	LeaderDiskID   string `json:"LeaderDiskId"`
	LeaderCellID   string `json:"LeaderCellId"`
	FollowerDiskID string `json:"FollowerDiskId"`
	FollowerCellID string `json:"FollowerCellId"`
}

func (l VolumeLink) Validate() error {
	if l.LeaderDiskID == "" || l.LeaderCellID == "" ||
		l.FollowerDiskID == "" || l.FollowerCellID == "" {
		return errors.NewNonRetriableErrorf("volume link disk and cell IDs must be specified")
	}
	if l.LeaderDiskID == l.FollowerDiskID && l.LeaderCellID == l.FollowerCellID {
		return errors.NewNonRetriableErrorf("volume link source and target must differ")
	}
	return nil
}

type VolumeLinkState string

const (
	VolumeLinkStateNone                   VolumeLinkState = "LINK_STATUS_NONE"
	VolumeLinkStateNotFound               VolumeLinkState = "LINK_STATUS_NOT_FOUND"
	VolumeLinkStatePreparing              VolumeLinkState = "LINK_STATUS_PREPARING"
	VolumeLinkStateLeadershipTransferring VolumeLinkState = "LINK_STATUS_LEADERSHIP_TRANSFERRING"
	VolumeLinkStateLeadershipTransferred  VolumeLinkState = "LINK_STATUS_LEADERSHIP_TRANSFERRED"
	VolumeLinkStateCompleted              VolumeLinkState = "LINK_STATUS_COMPLETED"
	VolumeLinkStateError                  VolumeLinkState = "LINK_STATUS_ERROR"
)

type VolumeLinkStatus struct {
	State        VolumeLinkState
	LinkUUID     string
	ErrorMessage string
}

const volumeLinkTaskIDTag = "volume-link-task-id"

func volumeLinkSourceID(link VolumeLink) string {
	if link.LeaderCellID == link.FollowerCellID {
		return link.LeaderDiskID
	}
	return link.LeaderCellID + "/" + link.LeaderDiskID
}

func (c *client) validateVolumeLink(link VolumeLink) error {
	if err := link.Validate(); err != nil {
		return err
	}
	if link.LeaderCellID != c.zoneID {
		return errors.NewNonRetriableErrorf(
			"volume link leader is in cell %q, client is for cell %q",
			link.LeaderCellID,
			c.zoneID,
		)
	}
	return nil
}

func (c *client) CreateVolumeLink(ctx context.Context, link VolumeLink) (err error) {
	defer c.metrics.StatRequest("CreateVolumeLink")(&err)
	if err := c.validateVolumeLink(link); err != nil {
		return err
	}
	return NormalizeVolumeLinkError(c.nbs.CreateVolumeLink(
		c.withTimeoutHeader(ctx),
		link.LeaderDiskID,
		link.FollowerDiskID,
		link.FollowerCellID,
	))
}

func (c *client) DestroyVolumeLink(ctx context.Context, link VolumeLink) (err error) {
	defer c.metrics.StatRequest("DestroyVolumeLink")(&err)
	if err := c.validateVolumeLink(link); err != nil {
		return err
	}
	return NormalizeVolumeLinkError(c.nbs.DestroyVolumeLink(
		c.withTimeoutHeader(ctx),
		link.LeaderDiskID,
		link.FollowerDiskID,
		link.FollowerCellID,
	))
}

func (c *client) GetVolumeLinkStatus(
	ctx context.Context,
	link VolumeLink,
) (status VolumeLinkStatus, err error) {

	defer c.metrics.StatRequest("GetVolumeLinkStatus")(&err)
	if err := c.validateVolumeLink(link); err != nil {
		return VolumeLinkStatus{}, err
	}
	// This action parses these exact field names rather than a protobuf request.
	input, err := json.Marshal(link)
	if err != nil {
		return VolumeLinkStatus{}, errors.NewNonRetriableError(err)
	}
	output, err := c.executeExactVolumeLinkAction(ctx, "getlinkstatus", input)
	if err != nil {
		return VolumeLinkStatus{}, err
	}
	return parseVolumeLinkStatus(output)
}

func parseVolumeLinkStatus(output []byte) (VolumeLinkStatus, error) {
	var result struct {
		Error        *core_protos.TError `json:"error"`
		Status       VolumeLinkState     `json:"status"`
		LinkUUID     string              `json:"linkUUID"`
		ErrorMessage string              `json:"errorMessage"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return VolumeLinkStatus{}, errors.NewNonRetriableErrorf(
			"failed to parse getlinkstatus response: %v", err,
		)
	}
	if result.Error != nil {
		err := &nbs_client.ClientError{Code: result.Error.Code, Message: result.Error.Message}
		if !err.Succeeded() {
			return VolumeLinkStatus{}, NormalizeVolumeLinkError(err)
		}
	}
	switch result.Status {
	case VolumeLinkStateNone, VolumeLinkStateNotFound, VolumeLinkStatePreparing,
		VolumeLinkStateLeadershipTransferring, VolumeLinkStateLeadershipTransferred,
		VolumeLinkStateCompleted, VolumeLinkStateError:
	default:
		return VolumeLinkStatus{}, errors.NewNonRetriableErrorf(
			"unknown volume link status %q", result.Status,
		)
	}
	return VolumeLinkStatus{
		State:        result.Status,
		LinkUUID:     result.LinkUUID,
		ErrorMessage: result.ErrorMessage,
	}, nil
}

func (c *client) describeExactVolume(ctx context.Context, diskID string) (*protos.TVolume, error) {
	response, err := c.nbs.Impl.DescribeVolume(c.withTimeoutHeader(ctx), &protos.TDescribeVolumeRequest{
		Headers: &protos.THeaders{ExactDiskIdMatch: true},
		DiskId:  diskID,
	})
	if err != nil {
		return nil, NormalizeVolumeLinkError(err)
	}
	if response.GetVolume().GetDiskId() != diskID {
		return nil, errors.NewNonRetriableErrorf("describe returned a different volume for %q", diskID)
	}
	return response.GetVolume(), nil
}

// CreateVolumeLinkTarget creates a physical follower without registering another
// logical disk in DM. Unlike Clone, it does not start a FillGeneration session.
func (f *factory) CreateVolumeLinkTarget(
	ctx context.Context,
	link VolumeLink,
	taskID string,
) error {

	if err := link.Validate(); err != nil {
		return err
	}
	if taskID == "" {
		return errors.NewNonRetriableErrorf("volume link task ID must be specified")
	}
	source, err := f.getClient(ctx, link.LeaderCellID)
	if err != nil {
		return err
	}
	target, err := f.getClient(ctx, link.FollowerCellID)
	if err != nil {
		return err
	}

	volume, err := source.describeExactVolume(ctx, link.LeaderDiskID)
	if err != nil {
		return err
	}
	// Overlay relocation and local-disk placement require separate orchestration.
	if volume.GetBaseDiskId() != "" {
		return errors.NewNonRetriableErrorf("transferring overlay volumes via a link is not supported yet")
	}
	if volume.GetStorageMediaKind() == core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_LOCAL ||
		volume.GetStorageMediaKind() == core_protos.EStorageMediaKind_STORAGE_MEDIA_HDD_LOCAL {
		return errors.NewNonRetriableErrorf("transferring local volumes via a link is not supported")
	}
	if volume.GetStorageMediaKind() == core_protos.EStorageMediaKind_STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP {
		return errors.NewNonRetriableErrorf("direct-mirror group transfers require destination storage-pool support")
	}

	existing, err := target.describeExactVolume(ctx, link.FollowerDiskID)
	if err == nil {
		return validateVolumeLinkTarget(existing, volume, link, taskID)
	}
	if !IsNotFoundError(err) {
		return err
	}

	encryptionDesc, err := getEncryptionDesc(volume.GetEncryptionDesc())
	if err != nil {
		return err
	}
	encryptionSpec, err := getEncryptionSpec(encryptionDesc)
	if err != nil {
		return err
	}
	err = target.createVolume(ctx, link.FollowerDiskID, volume.GetBlocksCount(), &nbs_client.CreateVolumeOpts{
		BlockSize:        volume.GetBlockSize(),
		StorageMediaKind: volume.GetStorageMediaKind(),
		CloudId:          volume.GetCloudId(),
		FolderId:         volume.GetFolderId(),
		TabletVersion:    volume.GetTabletVersion(),
		PartitionsCount:  volume.GetPartitionsCount(),
		IsSystem:         volume.GetIsSystem(),
		ProjectId:        volume.GetProjectId(),
		ChannelsCount:    volume.GetChannelsCount(),
		EncryptionSpec:   encryptionSpec,
		TagsStr: fmt.Sprintf("source-disk-id=%s,%s=%s",
			volumeLinkSourceID(link), volumeLinkTaskIDTag, taskID),
	})
	if err != nil {
		return NormalizeVolumeLinkError(err)
	}

	// Also check ownership after CreateVolume: another task may have created the
	// same name between DescribeVolume and CreateVolume.
	existing, err = target.describeExactVolume(ctx, link.FollowerDiskID)
	if err != nil {
		return err
	}
	return validateVolumeLinkTarget(existing, volume, link, taskID)
}

func validateVolumeLinkTarget(target, source *protos.TVolume, link VolumeLink, taskID string) error {
	if target.GetTags()[volumeLinkTaskIDTag] != taskID ||
		target.GetTags()["source-disk-id"] != volumeLinkSourceID(link) {
		return errors.NewNonRetriableErrorf(
			"target volume %s/%s does not belong to transfer task %q",
			link.FollowerCellID, link.FollowerDiskID, taskID,
		)
	}
	if target.GetBlockSize() != source.GetBlockSize() || target.GetBlocksCount() != source.GetBlocksCount() {
		return errors.NewNonRetriableErrorf("volume link target geometry differs from the source")
	}
	return nil
}

// ResolveVolumeLinkSource permits the normal NBS logical-name lookup. All
// subsequent transfer requests use the exact physical name returned here.
func (c *client) ResolveVolumeLinkSource(ctx context.Context, logicalDiskID string) (string, error) {
	volume, err := c.describeVolume(ctx, logicalDiskID)
	if err != nil {
		return "", NormalizeVolumeLinkError(err)
	}
	physicalDiskID := volume.GetDiskId()
	if physicalDiskID != logicalDiskID && physicalDiskID != logicalDiskID+"-copy" {
		return "", errors.NewNonRetriableErrorf(
			"logical disk %q resolved to unrelated volume %q", logicalDiskID, physicalDiskID,
		)
	}
	return physicalDiskID, nil
}

// DeleteVolumeLinkTarget is used only before the task persists its intent to
// create a link. An existing volume owned by another task is left untouched.
func (f *factory) DeleteVolumeLinkTarget(ctx context.Context, link VolumeLink, taskID string) error {
	if err := link.Validate(); err != nil {
		return err
	}
	target, err := f.getClient(ctx, link.FollowerCellID)
	if err != nil {
		return err
	}
	volume, err := target.describeExactVolume(ctx, link.FollowerDiskID)
	if IsNotFoundError(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if taskID == "" || volume.GetTags()[volumeLinkTaskIDTag] != taskID ||
		volume.GetTags()["source-disk-id"] != volumeLinkSourceID(link) {
		return nil
	}
	_, err = target.nbs.Impl.DestroyVolume(target.withTimeoutHeader(ctx), &protos.TDestroyVolumeRequest{
		Headers: &protos.THeaders{ExactDiskIdMatch: true},
		DiskId:  link.FollowerDiskID,
		Sync:    true,
	})
	if IsNotFoundError(err) {
		return nil
	}
	return NormalizeVolumeLinkError(err)
}

// FinalizeVolumeLinkTarget removes temporary discovery and ownership markers
// once COMPLETED has been persisted by the task. Repeating it is safe.
func (f *factory) FinalizeVolumeLinkTarget(ctx context.Context, link VolumeLink, taskID string) error {
	if err := link.Validate(); err != nil {
		return err
	}
	if taskID == "" {
		return errors.NewNonRetriableErrorf("volume link task ID must be specified")
	}
	target, err := f.getClient(ctx, link.FollowerCellID)
	if err != nil {
		return err
	}
	return target.finalizeVolumeLinkTarget(ctx, link, taskID)
}

// Keep normalization local to the new transfer adapter: existing NBS clients
// retain their historical error behavior.
func NormalizeVolumeLinkError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, errors.NewEmptyRetriableError()) || errors.Is(err, errors.NewEmptyNonRetriableError()) ||
		errors.Is(err, errors.NewEmptyNonCancellableError()) {
		return err
	}
	var clientError *nbs_client.ClientError
	if errors.As(err, &clientError) {
		wrapped := wrapError(err)
		if errors.Is(wrapped, errors.NewEmptyRetriableError()) {
			return wrapped
		}
		return errors.NewNonRetriableError(wrapped)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.NewRetriableError(err)
	}
	return err
}

// Only explicit validation/busy/unsupported-operation refusals are considered proof
// that no copying started. Transport and generic server failures remain unknown.
func IsVolumeLinkCreateRejected(err error) bool {
	var clientError *nbs_client.ClientError
	if !errors.As(err, &clientError) {
		return false
	}
	switch clientError.Code {
	case nbs_client.E_ARGUMENT, nbs_client.E_TRY_AGAIN, nbs_client.E_NOT_IMPLEMENTED, nbs_client.E_GRPC_UNIMPLEMENTED:
		return true
	default:
		return false
	}
}

func (c *client) executeExactVolumeLinkAction(ctx context.Context, action string, input []byte) ([]byte, error) {
	response, err := c.nbs.Impl.ExecuteAction(c.withTimeoutHeader(ctx), &protos.TExecuteActionRequest{
		Headers: &protos.THeaders{ExactDiskIdMatch: true}, Action: action, Input: input,
	})
	if err != nil {
		return nil, NormalizeVolumeLinkError(err)
	}
	if failure := response.GetError(); failure != nil {
		clientError := &nbs_client.ClientError{Code: failure.Code, Message: failure.Message}
		if clientError.Failed() {
			return nil, NormalizeVolumeLinkError(clientError)
		}
	}
	return response.GetOutput(), nil
}

func (c *client) finalizeVolumeLinkTarget(ctx context.Context, link VolumeLink, taskID string) error {
	for attempt := 0; ; attempt++ {
		volume, err := c.describeExactVolume(ctx, link.FollowerDiskID)
		if err != nil {
			return err
		}
		owner := volume.GetTags()[volumeLinkTaskIDTag]
		source := volume.GetTags()["source-disk-id"]
		if owner == "" && source == "" {
			return nil
		}
		if owner != taskID || (source != "" && source != volumeLinkSourceID(link)) {
			return errors.NewNonRetriableErrorf("target volume ownership changed before finalization")
		}
		request := &private_protos.TModifyTagsRequest{
			DiskId:        link.FollowerDiskID,
			ConfigVersion: volume.ConfigVersion,
			TagsToRemove: []string{
				"source-disk-id=" + volumeLinkSourceID(link),
				volumeLinkTaskIDTag + "=" + taskID,
			},
		}
		input, err := new(jsonpb.Marshaler).MarshalToString(request)
		if err != nil {
			return errors.NewNonRetriableError(err)
		}
		output, err := c.executeExactVolumeLinkAction(ctx, "ModifyTags", []byte(input))
		if err != nil {
			if isAbortedError(err) && attempt < maxConsecutiveRetries {
				continue
			}
			return err
		}
		response := &private_protos.TModifyTagsResponse{}
		if err := new(jsonpb.Unmarshaler).Unmarshal(strings.NewReader(string(output)), response); err != nil {
			return errors.NewNonRetriableError(err)
		}
		return nil
	}
}
