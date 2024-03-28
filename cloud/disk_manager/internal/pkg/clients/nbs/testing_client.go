package nbs

import (
	"context"
	"fmt"
	"hash/crc32"

	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func (c *client) CalculateCrc32(
	diskID string,
	contentSize uint64,
) (DiskContentInfo, error) {

	return c.CalculateCrc32WithEncryption(diskID, contentSize, nil)
}

func (c *client) CalculateCrc32WithEncryption(
	diskID string,
	contentSize uint64,
	encryption *types.EncryptionDesc,
) (DiskContentInfo, error) {

	ctx := setupStderrLogger(context.Background())

	nbsClient, _, err := c.nbs.DiscoverInstance(ctx)
	if err != nil {
		return DiskContentInfo{}, err
	}
	defer nbsClient.Close()

	session := nbs_client.NewSession(
		*nbsClient,
		NewNbsClientLog(nbs_client.LOG_DEBUG),
	)
	defer session.Close()

	encryptionSpec, err := getEncryptionSpec(encryption)
	if err != nil {
		return DiskContentInfo{}, err
	}

	opts := nbs_client.MountVolumeOpts{
		MountFlags:     protoFlags(protos.EMountFlag_MF_THROTTLING_DISABLED),
		MountSeqNumber: 0,
		AccessMode:     protos.EVolumeAccessMode_VOLUME_ACCESS_READ_ONLY,
		MountMode:      protos.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
		EncryptionSpec: encryptionSpec,
	}
	err = session.MountVolume(ctx, diskID, &opts)
	if err != nil {
		return DiskContentInfo{}, err
	}
	defer session.UnmountVolume(ctx)

	volume := session.Volume()

	volumeBlockSize := uint64(volume.BlockSize)
	if volumeBlockSize == 0 {
		return DiskContentInfo{}, fmt.Errorf(
			"%v volume block size should not be zero",
			diskID,
		)
	}

	if contentSize%volumeBlockSize != 0 {
		return DiskContentInfo{}, fmt.Errorf(
			"%v contentSize %v should be multiple of volumeBlockSize %v",
			diskID,
			contentSize,
			volumeBlockSize,
		)
	}

	contentBlocksCount := contentSize / volumeBlockSize
	volumeSize := volume.BlocksCount * volumeBlockSize

	if contentSize > volumeSize {
		return DiskContentInfo{}, fmt.Errorf(
			"%v contentSize %v should not be greater than volumeSize %v",
			diskID,
			contentSize,
			volumeSize,
		)
	}

	chunkSize := uint64(4 * 1024 * 1024)
	blocksInChunk := chunkSize / volumeBlockSize
	acc := crc32.NewIEEE()
	blockCrc32s := []uint32{}

	for offset := uint64(0); offset < contentBlocksCount; offset += blocksInChunk {
		blocksToRead := min(contentBlocksCount-offset, blocksInChunk)
		buffers, err := session.ReadBlocks(ctx, offset, uint32(blocksToRead), "")
		if err != nil {
			return DiskContentInfo{}, fmt.Errorf(
				"%v read blocks at (%v, %v) failed: %w",
				diskID,
				offset,
				blocksToRead,
				err,
			)
		}

		for _, buffer := range buffers {
			blockAcc := crc32.NewIEEE()
			if len(buffer) == 0 {
				buffer = make([]byte, volumeBlockSize)
			}

			_, err := acc.Write(buffer)
			if err != nil {
				return DiskContentInfo{}, err
			}

			_, err = blockAcc.Write(buffer)
			if err != nil {
				return DiskContentInfo{}, err
			}

			blockCrc32s = append(blockCrc32s, blockAcc.Sum32())
		}
	}

	// Validate that region outside of contentSize is filled with zeroes.
	for offset := contentBlocksCount; offset < volume.BlocksCount; offset += blocksInChunk {
		blocksToRead := min(volume.BlocksCount-offset, blocksInChunk)
		buffers, err := session.ReadBlocks(ctx, offset, uint32(blocksToRead), "")
		if err != nil {
			return DiskContentInfo{}, fmt.Errorf(
				"%v read blocks at (%v, %v) failed: %w",
				diskID,
				offset,
				blocksToRead,
				err,
			)
		}

		for i, buffer := range buffers {
			if len(buffer) != 0 {
				for j, b := range buffer {
					if b != 0 {
						return DiskContentInfo{}, fmt.Errorf(
							"%v non zero byte %v detected at (%v, %v)",
							diskID,
							b,
							offset+uint64(i),
							j,
						)
					}
				}
			}
		}
	}

	return DiskContentInfo{
		ContentSize: contentSize,
		Crc32:       acc.Sum32(),
		BlockCrc32s: blockCrc32s,
	}, nil
}

func (c *client) ValidateCrc32(
	ctx context.Context,
	diskID string,
	expectedDiskContentInfo DiskContentInfo,
) error {

	return c.ValidateCrc32WithEncryption(
		ctx,
		diskID,
		expectedDiskContentInfo,
		nil,
	)
}

func (c *client) ValidateCrc32WithEncryption(
	ctx context.Context,
	diskID string,
	expectedDiskContentInfo DiskContentInfo,
	encryption *types.EncryptionDesc,
) error {

	actualDiskContentInfo, err := c.CalculateCrc32WithEncryption(
		diskID,
		expectedDiskContentInfo.ContentSize,
		encryption,
	)
	if err != nil {
		return err
	}

	actualCrc32 := actualDiskContentInfo.Crc32
	expectedCrc32 := expectedDiskContentInfo.Crc32
	actualBlockCrc32s := actualDiskContentInfo.BlockCrc32s
	expectedBlockCrc32s := expectedDiskContentInfo.BlockCrc32s

	if len(actualBlockCrc32s) != len(expectedBlockCrc32s) {
		logging.Debug(
			ctx,
			"%v blocksCrc32 length doesn't match, expected %v, actual %v",
			diskID,
			len(expectedBlockCrc32s),
			len(actualBlockCrc32s),
		)
	} else {
		for i := range expectedDiskContentInfo.BlockCrc32s {
			if actualBlockCrc32s[i] != expectedBlockCrc32s[i] {
				logging.Debug(
					ctx,
					"%v block with index %v crc32 doesn't match, expected %v, actual %v",
					diskID,
					i,
					expectedBlockCrc32s[i],
					actualBlockCrc32s[i],
				)
			}
		}
	}

	if actualCrc32 != expectedCrc32 {
		return fmt.Errorf(
			"%v crc32 doesn't match, expected %v, actual %v",
			diskID,
			expectedCrc32,
			actualCrc32,
		)
	}

	return nil
}

func (c *client) MountForReadWrite(
	diskID string,
) (func(), error) {

	ctx := setupStderrLogger(context.Background())

	nbsClient, _, err := c.nbs.DiscoverInstance(ctx)
	if err != nil {
		return func() {}, err
	}

	session := nbs_client.NewSession(
		*nbsClient,
		NewNbsClientLog(nbs_client.LOG_DEBUG),
	)

	opts := nbs_client.MountVolumeOpts{
		MountFlags:     protoFlags(protos.EMountFlag_MF_THROTTLING_DISABLED),
		MountSeqNumber: 0,
		AccessMode:     protos.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		MountMode:      protos.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
	}
	err = session.MountVolume(ctx, diskID, &opts)
	if err != nil {
		session.Close()
		_ = nbsClient.Close()
		return func() {}, err
	}

	unmountFunc := func() {
		// Not interested in error.
		_ = session.UnmountVolume(ctx)
		session.Close()
		_ = nbsClient.Close()
	}
	return unmountFunc, nil
}

func (c *client) Write(
	diskID string,
	startIndex int,
	bytes []byte,
) error {

	ctx := setupStderrLogger(context.Background())

	nbsClient, _, err := c.nbs.DiscoverInstance(ctx)
	if err != nil {
		return err
	}
	defer nbsClient.Close()

	session := nbs_client.NewSession(
		*nbsClient,
		NewNbsClientLog(nbs_client.LOG_DEBUG),
	)
	defer session.Close()

	opts := nbs_client.MountVolumeOpts{
		MountFlags:     protoFlags(protos.EMountFlag_MF_THROTTLING_DISABLED),
		MountSeqNumber: 0,
		AccessMode:     protos.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		MountMode:      protos.EVolumeMountMode_VOLUME_MOUNT_REMOTE,
	}
	err = session.MountVolume(ctx, diskID, &opts)
	if err != nil {
		return err
	}
	defer session.UnmountVolume(ctx)

	volume := session.Volume()
	diskSize := int(volume.BlocksCount) * int(volume.BlockSize)
	blockSize := int(volume.BlockSize)

	if startIndex < 0 || startIndex+len(bytes) > diskSize {
		return fmt.Errorf("invalid write range=(%v, %v)", startIndex, len(bytes))
	}

	if startIndex%blockSize != 0 || len(bytes)%blockSize != 0 {
		return fmt.Errorf("startIndex and len(bytes) should be divisible by block size")
	}

	chunkSize := 1024 * blockSize

	for i := 0; i < len(bytes); i += chunkSize {
		blocks := make([][]byte, 0)
		end := int(min(uint64(len(bytes)), uint64(i+chunkSize)))

		for j := i; j < end; j += blockSize {
			buffer := make([]byte, blockSize)
			copy(buffer, bytes[j:j+len(buffer)])

			blocks = append(blocks, buffer)
		}

		err = session.WriteBlocks(ctx, uint64((i+startIndex)/blockSize), blocks)
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type checkpoint struct {
	CheckpointID string `json:"CheckpointId"`
	// We don't need other checkpoint fields.
}

type partitionInfo struct {
	Checkpoints []checkpoint `json:"Checkpoints"`
	// We don't need other partitionInfo fields.
}

func (c *client) GetCheckpoints(
	ctx context.Context,
	diskID string,
) ([]string, error) {
	return c.nbs.GetCheckpoints(ctx, diskID)
}

func (c *client) List(ctx context.Context) ([]string, error) {
	return c.nbs.ListVolumes(ctx)
}
