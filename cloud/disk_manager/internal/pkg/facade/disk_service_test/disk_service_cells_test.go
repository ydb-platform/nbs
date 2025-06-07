package disk_service_test

import (
	"testing"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type TestCase struct {
	name   string
	zoneId string
}

func cellsTestCases() []TestCase {
	return []TestCase{
		{
			name:   "Sharded zone",
			zoneId: shardedZoneId,
		},
		{
			name:   "Cell in sharded zone",
			zoneId: cellId1,
		},
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInCellsCreateEmptyDisk(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateEmptyDiskWithZoneID(
				t,
				testCase.zoneId,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInCellsCreateDiskFromImage(t *testing.T) {
	for _, testCase := range cellsTestCases() {
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

func TestDiskServiceInCellsCreateSsdNonreplDiskFromPooledImage(t *testing.T) {
	for _, testCase := range cellsTestCases() {
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
	for _, testCase := range cellsTestCases() {
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
	for _, testCase := range cellsTestCases() {
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

func TestDiskServiceInCellsCreateEncryptedSsdNonreplDiskFromImage(t *testing.T) {
	for _, testCase := range cellsTestCases() {
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

func TestDiskServiceInCellsCreateSsdNonreplDiskWithDefaultEncryptionFromImage(
	t *testing.T,
) {

	for _, testCase := range cellsTestCases() {
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

func TestDiskServiceInCellsCreateDiskFromImageWithForceNotLayered(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromImageWithForceNotLayeredWithZoneID(
				t,
				testCase.zoneId,
			)
		})
	}
}

func TestDiskServiceInCellsCancelCreateDiskFromImageWithZoneID(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCancelCreateDiskFromImageWithZoneID(
				t,
				testCase.zoneId,
			)
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestDiskServiceInCellsCreateDiskFromSnapshot(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromSnapshotWithZoneID(t, testCase.zoneId)
		})
	}
}

func TestDiskServiceInCellsCreateDiskFromIncrementalSnapshot(t *testing.T) {
	for _, testCase := range cellsTestCases() {
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

func TestDiskServiceInCellsCreateSsdNonreplDiskFromIncrementalSnapshot(t *testing.T) {
	for _, testCase := range cellsTestCases() {
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

func TestDiskServiceInCellsCreateDiskFromSnapshotOfOverlayDisk(t *testing.T) {
	for _, testCase := range cellsTestCases() {
		t.Run(testCase.name, func(t *testing.T) {
			testDiskServiceCreateDiskFromSnapshotOfOverlayDiskInZone(
				t,
				testCase.zoneId,
			)
		})
	}
}
