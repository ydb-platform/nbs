package nbs

import (
	"bytes"
	"context"
	"fmt"
	"hash/crc32"
	"math/rand"
	"time"

	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	nbs_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nbs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"golang.org/x/sync/errgroup"
)

////////////////////////////////////////////////////////////////////////////////

type testingClient struct {
	client *client
}

func newFactory(
	ctx context.Context,
	client_config *nbs_config.ClientConfig,
) (*factory, error) {

	return newFactoryWithCreds(
		ctx,
		client_config,
		nil, // creds
		metrics.NewEmptyRegistry(),
		metrics.NewEmptyRegistry(),
	)
}

func NewTestingClient(
	ctx context.Context,
	zoneID string,
	client_config *nbs_config.ClientConfig,
) (TestingClient, error) {

	factory, err := newFactory(ctx, client_config)
	if err != nil {
		return nil, err
	}

	c, err := factory.getClient(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	return &testingClient{client: c}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *testingClient) Ping(ctx context.Context) (err error) {
	return c.client.Ping(ctx)
}

func (c *testingClient) Create(
	ctx context.Context,
	params CreateDiskParams,
) (err error) {

	return c.client.Create(ctx, params)
}

func (c *testingClient) CreateProxyOverlayDisk(
	ctx context.Context,
	diskID string,
	baseDiskID string,
	baseDiskCheckpointID string,
) (created bool, err error) {

	return c.client.CreateProxyOverlayDisk(
		ctx,
		diskID,
		baseDiskID,
		baseDiskCheckpointID,
	)
}

func (c *testingClient) Delete(
	ctx context.Context,
	diskID string,
) (err error) {

	return c.client.Delete(ctx, diskID)
}

func (c *testingClient) DeleteSync(
	ctx context.Context,
	diskID string,
) (err error) {

	return c.client.DeleteSync(ctx, diskID)
}

func (c *testingClient) DeleteWithFillGeneration(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
) (err error) {

	return c.client.DeleteWithFillGeneration(ctx, diskID, fillGeneration)
}

func (c *testingClient) CreateCheckpoint(
	ctx context.Context,
	params CheckpointParams,
) (err error) {

	return c.client.CreateCheckpoint(ctx, params)
}

func (c *testingClient) GetCheckpointStatus(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (CheckpointStatus, error) {

	return c.client.GetCheckpointStatus(ctx, diskID, checkpointID)
}

func (c *testingClient) DeleteCheckpoint(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (err error) {

	return c.client.DeleteCheckpoint(ctx, diskID, checkpointID)
}

func (c *testingClient) DeleteCheckpointData(
	ctx context.Context,
	diskID string,
	checkpointID string,
) (err error) {

	return c.client.DeleteCheckpointData(ctx, diskID, checkpointID)
}

func (c *testingClient) Resize(
	ctx context.Context,
	checkpoint func() error,
	diskID string,
	size uint64,
) (err error) {

	return c.client.Resize(ctx, checkpoint, diskID, size)
}

func (c *testingClient) Alter(
	ctx context.Context,
	saveState func() error,
	diskID string,
	cloudID string,
	folderID string,
) (err error) {

	return c.client.Alter(ctx, saveState, diskID, cloudID, folderID)
}

func (c *testingClient) Rebase(
	ctx context.Context,
	saveState func() error,
	diskID string,
	baseDiskID string,
	targetBaseDiskID string,
) (err error) {

	return c.client.Rebase(ctx, saveState, diskID, baseDiskID, targetBaseDiskID)
}

func (c *testingClient) Assign(
	ctx context.Context,
	params AssignDiskParams,
) (err error) {

	return c.client.Assign(ctx, params)
}

func (c *testingClient) Unassign(
	ctx context.Context,
	diskID string,
) (err error) {

	return c.client.Unassign(ctx, diskID)
}

func (c *testingClient) DescribeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind types.DiskKind,
	tabletVersion uint32,
) (diskModel DiskModel, err error) {

	return c.client.DescribeModel(ctx, blocksCount, blockSize, kind, tabletVersion)
}

func (c *testingClient) Describe(
	ctx context.Context,
	diskID string,
) (diskParams DiskParams, err error) {

	return c.client.Describe(ctx, diskID)
}

func (c *testingClient) CreatePlacementGroup(
	ctx context.Context,
	groupID string,
	placementStrategy types.PlacementStrategy,
	placementPartitionCount uint32,
) (err error) {

	return c.client.CreatePlacementGroup(ctx, groupID, placementStrategy, placementPartitionCount)
}

func (c *testingClient) DeletePlacementGroup(
	ctx context.Context,
	groupID string,
) (err error) {

	return c.client.DeletePlacementGroup(ctx, groupID)
}

func (c *testingClient) AlterPlacementGroupMembership(
	ctx context.Context,
	saveState func() error,
	groupID string,
	placementPartitionIndex uint32,
	disksToAdd []string,
	disksToRemove []string,
) (err error) {

	return c.client.AlterPlacementGroupMembership(ctx, saveState, groupID, placementPartitionIndex, disksToAdd, disksToRemove)
}

func (c *testingClient) ListPlacementGroups(
	ctx context.Context,
) (groups []string, err error) {

	return c.client.ListPlacementGroups(ctx)
}

func (c *testingClient) DescribePlacementGroup(
	ctx context.Context,
	groupID string,
) (placementGroup PlacementGroup, err error) {

	return c.client.DescribePlacementGroup(ctx, groupID)
}

func (c *testingClient) MountRO(
	ctx context.Context,
	diskID string,
	encryption *types.EncryptionDesc,
) (session *Session, err error) {

	return c.client.MountRO(ctx, diskID, encryption)
}

func (c *testingClient) MountLocalRO(
	ctx context.Context,
	diskID string,
	encryption *types.EncryptionDesc,
) (session *Session, err error) {

	return c.client.MountLocalRO(ctx, diskID, encryption)
}

func (c *testingClient) MountRW(
	ctx context.Context,
	diskID string,
	fillGeneration uint64,
	fillSeqNumber uint64,
	encryption *types.EncryptionDesc,
) (session *Session, err error) {

	return c.client.MountRW(ctx, diskID, fillGeneration, fillSeqNumber, encryption)
}

func (c *testingClient) GetChangedBlocks(
	ctx context.Context,
	diskID string,
	startIndex uint64,
	blockCount uint32,
	baseCheckpointID,
	checkpointID string,
	ignoreBaseDisk bool,
) (blockMask []byte, err error) {

	return c.client.GetChangedBlocks(ctx, diskID, startIndex, blockCount, baseCheckpointID, checkpointID, ignoreBaseDisk)
}

func (c *testingClient) GetCheckpointSize(
	ctx context.Context,
	saveState func(blockIndex uint64, checkpointSize uint64) error,
	diskID string,
	checkpointID string,
	milestoneBlockIndex uint64,
	milestoneCheckpointSize uint64,
) (err error) {

	return c.client.GetCheckpointSize(ctx, saveState, diskID, checkpointID, milestoneBlockIndex, milestoneCheckpointSize)
}

func (c *testingClient) GetChangedBytes(
	ctx context.Context,
	diskID string,
	baseCheckpointID string,
	checkpointID string,
	ignoreBaseDisk bool,
) (diff uint64, err error) {

	return c.client.GetChangedBytes(ctx, diskID, baseCheckpointID, checkpointID, ignoreBaseDisk)
}

func (c *testingClient) Stat(
	ctx context.Context,
	diskID string,
) (stats DiskStats, err error) {

	return c.client.Stat(ctx, diskID)
}

func (c *testingClient) Freeze(
	ctx context.Context,
	saveState func() error,
	diskID string,
) (err error) {

	return c.client.Freeze(ctx, saveState, diskID)
}

func (c *testingClient) Unfreeze(
	ctx context.Context,
	saveState func() error,
	diskID string,
) (err error) {

	return c.client.Unfreeze(ctx, saveState, diskID)
}

func (c *testingClient) ScanDisk(
	ctx context.Context,
	diskID string,
	batchSize uint32,
) (err error) {

	return c.client.ScanDisk(ctx, diskID, batchSize)
}

func (c *testingClient) GetScanDiskStatus(
	ctx context.Context,
	diskID string,
) (progress ScanDiskStatus, err error) {

	return c.client.GetScanDiskStatus(ctx, diskID)
}

func (c *testingClient) FinishFillDisk(
	ctx context.Context,
	saveState func() error,
	diskID string,
	fillGeneration uint64,
) (err error) {

	return c.client.FinishFillDisk(ctx, saveState, diskID, fillGeneration)
}

func (c *testingClient) FillDisk(
	ctx context.Context,
	diskID string,
	contentSize uint64,
) (DiskContentInfo, error) {

	return c.FillEncryptedDisk(ctx, diskID, contentSize, nil)
}

func (c *testingClient) FillEncryptedDisk(
	ctx context.Context,
	diskID string,
	contentSize uint64,
	encryption *types.EncryptionDesc,
) (DiskContentInfo, error) {

	session, err := c.MountRW(
		ctx,
		diskID,
		0, // fillGeneration
		0, // fillSeqNumber
		encryption,
	)
	if err != nil {
		return DiskContentInfo{}, err
	}
	defer session.Close(ctx)

	chunkSize := uint64(1024 * 4096) // 4 MiB
	blockSize := uint64(session.BlockSize())
	blocksInChunk := uint32(chunkSize / blockSize)
	storageSize := uint64(0)
	zeroes := make([]byte, chunkSize)

	rand.Seed(time.Now().UnixNano())

	acc := crc32.NewIEEE()
	blockCrc32s := []uint32{}
	for offset := uint64(0); offset < contentSize; offset += chunkSize {
		startIndex := offset / blockSize
		data := make([]byte, chunkSize)
		dice := rand.Intn(3)

		var err error
		switch dice {
		case 0:
			rand.Read(data)
			if bytes.Equal(data, zeroes) {
				logging.Debug(ctx, "rand generated all zeroes")
			}

			err = session.Write(ctx, startIndex, data)
			storageSize += chunkSize
		case 1:
			err = session.Zero(ctx, startIndex, blocksInChunk)
		}
		if err != nil {
			return DiskContentInfo{}, err
		}

		_, err = acc.Write(data)
		if err != nil {
			return DiskContentInfo{}, err
		}

		for blockIndex := uint32(0); blockIndex < blocksInChunk; blockIndex++ {
			blockAcc := crc32.NewIEEE()

			startOffset := uint64(blockIndex) * blockSize
			endOffset := uint64(blockIndex+1) * blockSize
			blockData := data[startOffset:endOffset]

			_, err = blockAcc.Write(blockData)
			if err != nil {
				return DiskContentInfo{}, err
			}

			logging.Debug(
				ctx,
				"%v block with index %v crc32 now is %v",
				diskID,
				startIndex+uint64(blockIndex),
				blockAcc.Sum32(),
			)
			blockCrc32s = append(blockCrc32s, blockAcc.Sum32())
		}
	}

	return DiskContentInfo{
		ContentSize: contentSize,
		StorageSize: storageSize,
		Crc32:       acc.Sum32(),
		BlockCrc32s: blockCrc32s,
	}, nil
}

func (c *testingClient) GoWriteRandomBlocksToNbsDisk(
	ctx context.Context,
	diskID string,
) (func() error, error) {

	session, err := c.MountRW(
		ctx,
		diskID,
		0,   // fillGeneration
		0,   // fillSeqNumber
		nil, // encryption
	)
	if err != nil {
		return nil, err
	}

	errGroup := new(errgroup.Group)

	errGroup.Go(func() error {
		defer session.Close(ctx)

		writeCount := uint32(1000)

		blockSize := session.BlockSize()
		blocksCount := session.BlockCount()
		zeroes := make([]byte, blockSize)

		rand.Seed(time.Now().UnixNano())

		for i := uint32(0); i < writeCount; i++ {
			blockIndex := uint64(rand.Int63n(int64(blocksCount)))
			dice := rand.Intn(2)

			var err error
			blockAcc := crc32.NewIEEE()
			data := make([]byte, blockSize)

			switch dice {
			case 0:
				rand.Read(data)
				if bytes.Equal(data, zeroes) {
					logging.Debug(ctx, "rand generated all zeroes")
				}

				err = session.Write(ctx, blockIndex, data)
			case 1:
				err = session.Zero(ctx, blockIndex, 1)
			}

			if err != nil {
				return err
			}

			_, err = blockAcc.Write(data)
			if err != nil {
				return err
			}

			logging.Debug(
				ctx,
				"%v block with index %v crc32 now is %v",
				diskID,
				blockIndex,
				blockAcc.Sum32(),
			)
		}

		return nil
	})

	return errGroup.Wait, nil
}

func (c *testingClient) CalculateCrc32(
	diskID string,
	contentSize uint64,
) (DiskContentInfo, error) {

	return c.CalculateCrc32WithEncryption(diskID, contentSize, nil)
}

func (c *testingClient) CalculateCrc32WithEncryption(
	diskID string,
	contentSize uint64,
	encryption *types.EncryptionDesc,
) (DiskContentInfo, error) {

	ctx := setupStderrLogger(context.Background())

	nbsClient, _, err := c.client.nbs.DiscoverInstance(ctx)
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

func (c *testingClient) ValidateCrc32(
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

func (c *testingClient) ValidateCrc32WithEncryption(
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

func (c *testingClient) MountForReadWrite(
	diskID string,
) (func(), error) {

	ctx := setupStderrLogger(context.Background())

	nbsClient, _, err := c.client.nbs.DiscoverInstance(ctx)
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

func (c *testingClient) Write(
	diskID string,
	startIndex int,
	bytes []byte,
) error {

	ctx := setupStderrLogger(context.Background())

	nbsClient, _, err := c.client.nbs.DiscoverInstance(ctx)
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

func (c *testingClient) GetCheckpoints(
	ctx context.Context,
	diskID string,
) ([]string, error) {

	return c.client.nbs.GetCheckpoints(ctx, diskID)
}

func (c *testingClient) List(ctx context.Context) ([]string, error) {
	return c.client.nbs.ListVolumes(ctx)
}
