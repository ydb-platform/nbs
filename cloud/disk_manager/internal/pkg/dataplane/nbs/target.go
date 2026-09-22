package nbs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type DiskTarget interface {
	common.Target

	// Returns the mounted disk's exact size in bytes.
	Size() uint64
}

type diskTarget struct {
	client           nbs.Client
	session          *nbs.Session
	blockSize        uint32
	blockCount       uint64
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

	startIndex := uint64(chunk.Index) * t.blocksInChunk
	if startIndex >= t.blockCount {
		return errors.NewNonRetriableErrorf(
			"chunk %v starts beyond the disk: blockCount=%v",
			chunk.Index,
			t.blockCount,
		)
	}

	logging.Debug(ctx, "writing chunk %v", chunk.Index)

	blockCount := min(t.blocksInChunk, t.blockCount-startIndex)

	var err error
	if chunk.Zero {
		err = t.session.Zero(ctx, startIndex, uint32(blockCount))
	} else {
		dataSize := blockCount * uint64(t.blockSize)
		if uint64(len(chunk.Data)) < dataSize {
			return errors.NewNonRetriableErrorf(
				"chunk %v buffer is too small: size=%v, expected at least %v",
				chunk.Index,
				len(chunk.Data),
				dataSize,
			)
		}
		err = t.session.Write(ctx, startIndex, chunk.Data[:dataSize])
	}

	return err
}

func (t *diskTarget) Size() uint64 {
	return t.blockCount * uint64(t.blockSize)
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
) (DiskTarget, error) {

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
		return nil, err
	}

	blockSize := session.BlockSize()

	err = validate(chunkSize, blockSize)
	if err != nil {
		session.Close(ctx)
		return nil, err
	}

	return &diskTarget{
		client:           client,
		session:          session,
		blockSize:        blockSize,
		blockCount:       session.BlockCount(),
		blocksInChunk:    uint64(chunkSize / blockSize),
		ignoreZeroChunks: ignoreZeroChunks,
	}, nil
}
