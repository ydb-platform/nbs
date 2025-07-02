package nbs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type diskTarget struct {
	client           nbs.Client
	session          *nbs.Session
	blocksInChunk    uint64
	ignoreZeroChunks bool
}

func (t *diskTarget) Write(
	ctx context.Context,
	chunk common.Chunk,
) error {

	if t.ignoreZeroChunks && chunk.Zero {
		logging.Debug(ctx, "ignoring chunk %v", chunk.Index)
		return nil
	}

	logging.Debug(ctx, "writing chunk %v", chunk.Index)

	startIndex := uint64(chunk.Index) * t.blocksInChunk

	var err error
	if chunk.Zero {
		// blockCount should be multiple of blocksInChunk.
		err = t.session.Zero(ctx, startIndex, uint32(t.blocksInChunk))
	} else {
		// TODO: normalize chunk data.
		err = t.session.Write(ctx, startIndex, chunk.Data)
	}

	return err
}

func (t *diskTarget) Close(ctx context.Context) {
	t.session.Close(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func NewDiskTarget(
	ctx context.Context,
	factory nbs.Factory,
	disk *types.Disk,
	encryption *types.EncryptionDesc,
	chunkSize uint32,
	ignoreZeroChunks bool,
	fillGeneration uint64,
	fillSeqNumber uint64,
) (common.Target, error) {

	client, err := factory.GetClient(ctx, disk.ZoneId)
	if err != nil {
		return nil, err
	}

	session, err := client.MountRW(
		ctx,
		disk.DiskId,
		fillGeneration,
		fillSeqNumber,
		encryption,
	)
	if err != nil {
		// Checking if disk still exists.
		_, describeErr := client.Describe(ctx, disk.DiskId)
		if describeErr != nil {
			return nil, describeErr
		}

		return nil, err
	}

	blockSize := session.BlockSize()

	err = validate(session.BlockCount(), chunkSize, blockSize)
	if err != nil {
		session.Close(ctx)
		return nil, err
	}

	return &diskTarget{
		client:           client,
		session:          session,
		blocksInChunk:    uint64(chunkSize / blockSize),
		ignoreZeroChunks: ignoreZeroChunks,
	}, nil
}
