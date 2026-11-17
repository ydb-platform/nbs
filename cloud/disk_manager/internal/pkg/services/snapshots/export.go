package snapshots

import (
	"context"
	"net/url"
	"strings"

	"github.com/golang/protobuf/proto"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

func (s *service) ExportSnapshot(
	ctx context.Context,
	req *api.ExportSnapshotRequest,
) (string, error) {

	if s.exportConfig == nil {
		return "", errors.NewNonRetriableErrorf(
			"snapshot export is not configured",
		)
	}

	if req.GetSnapshotId() == "" || req.GetFolderId() == "" ||
		req.GetDst().GetBucket() == "" {
		return "", common.NewInvalidArgumentError(
			"snapshot_id, folder_id and destination bucket are required",
		)
	}

	if headers.GetTaskID(ctx) == "" {
		return "", common.NewInvalidArgumentError(
			"x-task-id must contain the Compute export task ID",
		)
	}

	endpoint, err := url.Parse(req.GetDst().GetEndpoint())
	if err != nil ||
		(endpoint.Scheme != "https" && endpoint.Scheme != "http") ||
		endpoint.Host == "" || endpoint.User != nil ||
		endpoint.RawQuery != "" || endpoint.Fragment != "" {

		return "", common.NewInvalidArgumentError("invalid S3 endpoint")
	}

	info := req.GetDecryptionInfo()
	if info != nil && (info.KekId == "" ||
		len(info.EncryptedDek) == 0) {
		return "", common.NewInvalidArgumentError(
			"decryption_info requires kek_id and encrypted_dek",
		)
	}

	// Use the same destination for all attempts. Compute prevents concurrent
	// exports to the same bucket and prefix.
	request := proto.Clone(req).(*api.ExportSnapshotRequest)
	endpoint.Host = strings.ToLower(endpoint.Host)
	endpoint.Path = strings.TrimRight(endpoint.Path, "/")
	request.Dst.Endpoint = endpoint.String()
	request.Dst.Prefix = strings.Trim(request.Dst.Prefix, "/")
	return s.taskScheduler.ScheduleTask(
		ctx,
		"snapshots.ExportSnapshot",
		"",
		request,
	)
}
