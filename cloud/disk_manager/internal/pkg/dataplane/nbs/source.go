package nbs

import (
	"context"
	"math"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const (
	maxChangedByteCountPerIteration = uint64((1 << 20) * 4096)
)

////////////////////////////////////////////////////////////////////////////////

type DiskSource interface {
	dataplane_common.Source

	// Returns the mounted disk's exact size in bytes, excluding chunk padding.
	Size() uint64
}

type diskSource struct {
	client           nbs.Client
	session          *nbs.Session
	diskID           string
	baseCheckpointID string
	checkpointID     string
	blockSize        uint32
	blocksInChunk    uint64
	blockCount       uint64
	chunkCount       uint32
	encryptionDesc   *types.EncryptionDesc

	maxChangedBlockCountPerIteration uint64
	duplicateChunkIndices            bool

	chunkIndices           common.ChannelWithInflightQueue
	duplicatedChunkIndices common.ChannelWithCancellation

	ignoreBaseDisk         bool
	dontReadFromCheckpoint bool
}

func (s *diskSource) sendChunkIndex(
	ctx context.Context,
	chunkIndex uint32,
) error {

	_, err := s.chunkIndices.Send(ctx, chunkIndex)
	if err != nil {
		return err
	}

	if !s.duplicatedChunkIndices.Empty() {
		_, err = s.duplicatedChunkIndices.Send(ctx, chunkIndex)
		return err
	}

	return nil
}

func (s *diskSource) generateChunkIndicesDefault(
	ctx context.Context,
	milestoneChunkIndex uint32,
) error {

	for chunkIndex := milestoneChunkIndex; chunkIndex < s.chunkCount; chunkIndex++ {
		err := s.sendChunkIndex(ctx, chunkIndex)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *diskSource) generateChunkIndicesUsingGetChangedBlocks(
	ctx context.Context,
	milestoneChunkIndex uint32,
) error {

	totalBlockCount := s.blockCount

	blockIndex := uint64(milestoneChunkIndex) * s.blocksInChunk
	for blockIndex < totalBlockCount {
		blockCount := uint32(s.maxChangedBlockCountPerIteration)
		if uint64(blockCount) > totalBlockCount-blockIndex {
			blockCount = uint32(totalBlockCount - blockIndex)
		}

		blockMask, err := s.client.GetChangedBlocks(
			ctx,
			s.diskID,
			blockIndex,
			blockCount,
			s.baseCheckpointID,
			s.checkpointID,
			s.ignoreBaseDisk,
		)
		if err != nil {
			return err
		}

		chunkIndex := uint32(blockIndex / s.blocksInChunk)

		// The last byte can contain bits beyond the end of the disk.
		maskSize := (int(blockCount) + 7) / 8
		if len(blockMask) > maskSize {
			blockMask = blockMask[:maskSize]
		}

		i := 0
		for i < len(blockMask) {
			// blocksInChunk should be multiple of 8.
			chunkEnd := i + int(s.blocksInChunk/8)

			for i < len(blockMask) && i < chunkEnd {
				mask := blockMask[i]
				if i == maskSize-1 && blockCount%8 != 0 {
					mask &= byte((1 << (blockCount % 8)) - 1)
				}

				if mask != 0 {
					err := s.sendChunkIndex(ctx, chunkIndex)
					if err != nil {
						return err
					}

					i = chunkEnd
					break
				}

				i++
			}

			chunkIndex++
		}

		s.chunkIndices.UpdateMilestoneHint(chunkIndex)
		blockIndex += s.maxChangedBlockCountPerIteration
	}

	return nil
}

func (s *diskSource) ChunkIndices(
	ctx context.Context,
	milestone dataplane_common.Milestone,
	processedChunkIndices <-chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan uint32, common.ChannelWithCancellation, <-chan error) {

	common.Assert(s.chunkIndices.Empty(), "should be called once")

	inflightLimit := cap(processedChunkIndices)

	s.chunkIndices = common.NewChannelWithInflightQueue(
		common.Milestone{
			Value:               milestone.ChunkIndex,
			ProcessedValueCount: milestone.TransferredChunkCount,
		},
		processedChunkIndices,
		holeChunkIndices,
		inflightLimit,
	)

	if s.duplicateChunkIndices {
		s.duplicatedChunkIndices = common.NewChannelWithCancellation(
			inflightLimit,
		)
	}

	errors := make(chan error, 1)

	go func() {
		defer close(errors)

		defer func() {
			if r := recover(); r != nil {
				errors <- task_errors.NewPanicError(r)
			}
		}()

		defer s.chunkIndices.Close()
		defer s.duplicatedChunkIndices.Close()

		err := s.generateChunkIndicesUsingGetChangedBlocks(
			ctx,
			milestone.ChunkIndex,
		)
		if err != nil && nbs.IsNotImplementedError(err) {
			logging.Info(
				ctx,
				"falling back to generateChunkIndicesDefault for disk %v: %v",
				s.diskID,
				err,
			)
			err = s.generateChunkIndicesDefault(ctx, milestone.ChunkIndex)
		}
		if err != nil {
			errors <- err
		}
	}()

	return s.chunkIndices.Channel(), s.duplicatedChunkIndices, errors
}

func (s *diskSource) Read(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) error {

	logging.Debug(ctx, "reading chunk %v", chunk.Index)

	startIndex := uint64(chunk.Index) * s.blocksInChunk
	if startIndex >= s.blockCount {
		return task_errors.NewNonRetriableErrorf(
			"chunk %v starts beyond the disk: blockCount=%v",
			chunk.Index,
			s.blockCount,
		)
	}

	blockCount := min(s.blocksInChunk, s.blockCount-startIndex)
	dataSize := blockCount * uint64(s.blockSize)
	if uint64(len(chunk.Data)) < s.blocksInChunk*uint64(s.blockSize) {
		return task_errors.NewNonRetriableErrorf(
			"chunk %v buffer is too small: size=%v",
			chunk.Index,
			len(chunk.Data),
		)
	}

	// Transfer reuses buffers. Padding must not contain a previous chunk's data.
	clear(chunk.Data[dataSize:])
	chunk.Zero = false

	checkpointID := s.checkpointID
	if s.dontReadFromCheckpoint {
		// Use the flag to handle checkpoints without data.
		// Reading from an empty checkpoint retrieves the latest data.
		checkpointID = ""
	}

	return s.session.Read(
		ctx,
		startIndex,
		uint32(blockCount),
		checkpointID,
		chunk.Data[:dataSize],
		&chunk.Zero,
	)
}

func (s *diskSource) Milestone() dataplane_common.Milestone {
	common.Assert(!s.chunkIndices.Empty(), "should not be empty")

	milestone := s.chunkIndices.Milestone()
	return dataplane_common.Milestone{
		ChunkIndex:            milestone.Value,
		TransferredChunkCount: milestone.ProcessedValueCount,
	}
}

func (s *diskSource) ChunkCount(ctx context.Context) (uint32, error) {
	return s.chunkCount, nil
}

func (s *diskSource) Size() uint64 {
	return s.blockCount * uint64(s.blockSize)
}

func (s *diskSource) EstimatedBytesToRead(ctx context.Context) (uint64, error) {
	diff, err := s.client.GetChangedBytes(
		ctx,
		s.diskID,
		s.baseCheckpointID,
		s.checkpointID,
		s.ignoreBaseDisk,
	)
	if err != nil && nbs.IsNotImplementedError(err) {
		// Worst case estimate
		return s.blockCount * uint64(s.blockSize), nil
	}

	return diff, err
}

func (s *diskSource) Close(ctx context.Context) {
	s.session.Close(ctx)
}

////////////////////////////////////////////////////////////////////////////////

// Data will be read from proxy disk.
func NewDiskSource(
	ctx context.Context,
	client nbs.Client,
	diskID string,
	proxyDiskID string,
	baseCheckpointID string,
	checkpointID string,
	encryption *types.EncryptionDesc,
	chunkSize uint32,
	duplicateChunkIndices bool,
	ignoreBaseDisk bool,
	dontReadFromCheckpoint bool,
) (DiskSource, error) {

	var session *nbs.Session
	var err error
	if len(proxyDiskID) != 0 {
		session, err = client.MountLocalRO(ctx, proxyDiskID, encryption)
	} else {
		session, err = client.MountRO(ctx, diskID, encryption)
	}

	if err != nil {
		return nil, err
	}

	blockSize := session.BlockSize()
	blockCount := session.BlockCount()

	err = validate(chunkSize, blockSize)
	if err != nil {
		session.Close(ctx)
		return nil, err
	}

	blocksInChunk := uint64(chunkSize / blockSize)
	if blocksInChunk%8 != 0 {
		session.Close(ctx)

		return nil, task_errors.NewNonRetriableErrorf(
			"blocksInChunk should be multiple of 8, blocksInChunk=%v",
			blocksInChunk,
		)
	}

	maxChangedBlockCountPerIteration :=
		maxChangedByteCountPerIteration / uint64(blockSize)

	if maxChangedBlockCountPerIteration%blocksInChunk != 0 {
		session.Close(ctx)

		return nil, task_errors.NewNonRetriableErrorf(
			"maxChangedBlockCountPerIteration should be multiple of blocksInChunk, maxChangedBlockCountPerIteration=%v, blocksInChunk=%v",
			maxChangedBlockCountPerIteration,
			blocksInChunk,
		)
	}

	chunkCount := blockCount / blocksInChunk
	if blockCount%blocksInChunk != 0 {
		chunkCount++
	}
	if chunkCount > math.MaxUint32 {
		session.Close(ctx)
		return nil, task_errors.NewNonRetriableErrorf(
			"disk has too many chunks: chunkCount=%v",
			chunkCount,
		)
	}

	return &diskSource{
		client:           client,
		session:          session,
		diskID:           diskID,
		baseCheckpointID: baseCheckpointID,
		checkpointID:     checkpointID,
		blockSize:        blockSize,
		blocksInChunk:    blocksInChunk,
		blockCount:       blockCount,
		chunkCount:       uint32(chunkCount),
		encryptionDesc:   encryption,

		maxChangedBlockCountPerIteration: maxChangedBlockCountPerIteration,
		duplicateChunkIndices:            duplicateChunkIndices,

		ignoreBaseDisk:         ignoreBaseDisk,
		dontReadFromCheckpoint: dontReadFromCheckpoint,
	}, nil
}
