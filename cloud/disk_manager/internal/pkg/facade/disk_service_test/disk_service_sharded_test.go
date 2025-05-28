package disk_service_test

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	internal_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/client"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/facade/testcommon"
)

////////////////////////////////////////////////////////////////////////////////

const (
	excludedFolderId = "excluded-folder"
	includedFolderId = "included-folder"
)

////////////////////////////////////////////////////////////////////////////////

type TestCase struct {
	name   string
	zoneId string
}

func shardsTestCases() []TestCase {
	return []TestCase{
		{
			name:   "Sharded zone",
			zoneId: shardedZoneId,
		},
		{
			name:   "Scale unit in sharded zone",
			zoneId: shardId1,
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInShardsCreateEmptyDisk(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateEmptyDiskWithZoneID(
				t,
				testCase.zoneId,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInShardsCreateDiskFromImage(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD,
				32*1024*4096, // imageSize
				false,        // pooled
				32*1024*4096, // diskSize
				"folder",
				nil, // encryptionDesc
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInShardsCreateSsdNonreplDiskFromPooledImage(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
				32*1024*4096, // imageSize
				true,         // pooled
				262144*4096,  // diskSize
				"folder",
				nil, // encryptionDesc
				testCase.zoneId,
			)
		})
	}
}

/*
// TODO: enable after issue #3071 has been completed
func TestDiskServiceCreateSsdNonreplDiskWithDefaultEncryptionFromPooledImage(
	t *testing.T,
) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
				32*1024*4096, // imageSize
				true,         // pooled
				262144*4096,  // diskSize
				"encrypted-folder",
				nil, // encryptionDesc
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceCreateEncryptedSsdNonreplDiskFromPooledImage(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
				32*1024*4096, // imageSize
				true,         // pooled
				262144*4096,  // diskSize
				"folder",
				&disk_manager.EncryptionDesc{
					Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
					Key: &disk_manager.EncryptionDesc_KmsKey{
						KmsKey: &disk_manager.KmsKey{
							KekId:        "kekid",
							EncryptedDek: []byte("encrypteddek"),
							TaskId:       "taskid",
						},
					},
				},
				testCase.zoneId,
			)
		})
	}
}
*/

func TestDiskServiceInShardsCreateEncryptedSsdNonreplDiskFromImage(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {

			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
				32*1024*4096, // imageSize
				false,        // pooled
				262144*4096,  // diskSize
				"folder",
				&disk_manager.EncryptionDesc{
					Mode: disk_manager.EncryptionMode_ENCRYPTION_AES_XTS,
					Key: &disk_manager.EncryptionDesc_KmsKey{
						KmsKey: &disk_manager.KmsKey{
							KekId:        "kekid",
							EncryptedDek: []byte("encrypteddek"),
							TaskId:       "taskid",
						},
					},
				},
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInShardsCreateSsdNonreplDiskWithDefaultEncryptionFromImage(
	t *testing.T,
) {

	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromImage(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
				32*1024*4096, // imageSize
				false,        // pooled
				262144*4096,  // diskSize
				"encrypted-folder",
				nil, // encryptionDesc
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInShardsCreateDiskFromImageWithForceNotLayered(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromImageWithForceNotLayeredWithZoneID(
				t,
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInShardsCancelCreateDiskFromImageWithZoneID(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCancelCreateDiskFromImageWithZoneID(
				t,
				testCase.zoneId,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInShardsCreateDiskFromSnapshot(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromSnapshotWithZoneID(t, testCase.zoneId)
		})
	}
}

func TestDiskServiceInShardsCreateDiskFromIncrementalSnapshot(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromIncrementalSnapshot(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD,
				128*1024*1024,
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInShardsCreateSsdNonreplDiskFromIncrementalSnapshot(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testCreateDiskFromIncrementalSnapshot(
				t,
				disk_manager.DiskKind_DISK_KIND_SSD_NONREPLICATED,
				262144*4096,
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInShardsCreateDiskFromSnapshotOfOverlayDisk(t *testing.T) {
	for _, testCase := range shardsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromSnapshotOfOverlayDiskInZone(
				t,
				testCase.zoneId,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInShardsCreateDiskInCorrectShard(t *testing.T) {
	ctx := testcommon.NewContext()

	client, err := testcommon.NewClient(ctx)
	require.NoError(t, err)
	defer client.Close()

	diskID1 := t.Name() + "1"

	reqCtx := testcommon.GetRequestContext(t, ctx)
	operation, err := client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 32 * 1024 * 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: shardedZoneId,
			DiskId: diskID1,
		},
		FolderId: excludedFolderId,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
	diskMeta, err := testcommon.GetDiskMeta(ctx, diskID1)
	require.NoError(t, err)
	require.Equal(t, shardId1, diskMeta.ZoneID)

	diskID2 := t.Name() + "2"

	reqCtx = testcommon.GetRequestContext(t, ctx)
	operation, err = client.CreateDisk(reqCtx, &disk_manager.CreateDiskRequest{
		Src: &disk_manager.CreateDiskRequest_SrcEmpty{
			SrcEmpty: &empty.Empty{},
		},
		Size: 32 * 1024 * 4096,
		Kind: disk_manager.DiskKind_DISK_KIND_SSD,
		DiskId: &disk_manager.DiskId{
			ZoneId: shardedZoneId,
			DiskId: diskID2,
		},
		FolderId: includedFolderId,
	})
	require.NoError(t, err)
	require.NotEmpty(t, operation)
	err = internal_client.WaitOperation(ctx, client, operation.Id)
	require.NoError(t, err)
	diskMeta, err = testcommon.GetDiskMeta(ctx, diskID2)
	require.NoError(t, err)
	require.Equal(t, shardId2, diskMeta.ZoneID)
}
