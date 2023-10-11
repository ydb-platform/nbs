package snapshot

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
)

////////////////////////////////////////////////////////////////////////////////

type snapshotShallowSource struct {
	source        *snapshotSource
	dstSnapshotID string
}

func (s *snapshotShallowSource) ChunkIndices(
	ctx context.Context,
	milestone dataplane_common.Milestone,
	processedChunkIndices <-chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan uint32, common.ChannelWithCancellation, <-chan error) {

	return s.source.ChunkIndices(
		ctx,
		milestone,
		processedChunkIndices,
		holeChunkIndices,
	)
}

func (s *snapshotShallowSource) ShallowCopy(
	ctx context.Context,
	chunkIndex uint32,
) error {

	return s.source.shallowCopy(ctx, chunkIndex, s.dstSnapshotID)
}

func (s *snapshotShallowSource) Milestone() dataplane_common.Milestone {
	return s.source.Milestone()
}

func (s *snapshotShallowSource) Close(ctx context.Context) {
	s.source.Close(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func NewSnapshotShallowSource(
	srcSnapshotID string,
	dstSnapshotID string,
	snapshotStorage storage.Storage,
) dataplane_common.ShallowSource {

	return &snapshotShallowSource{
		source:        newSnapshotSource(srcSnapshotID, snapshotStorage),
		dstSnapshotID: dstSnapshotID,
	}
}
