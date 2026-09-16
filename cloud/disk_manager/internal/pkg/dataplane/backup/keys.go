package backup

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////

func ChunkKey(keyPrefix string, chunkID string) string {
	return key(keyPrefix, fmt.Sprintf("chunks/%v", chunkID))
}

func MetaKey(keyPrefix string, diskID string, snapshotID string) string {
	return key(keyPrefix, snapshotDir(diskID, snapshotID)+"/meta.json")
}

func ChunkMapKey(keyPrefix string, diskID string, snapshotID string) string {
	return key(keyPrefix, snapshotDir(diskID, snapshotID)+"/map.bin")
}

////////////////////////////////////////////////////////////////////////////////

func key(keyPrefix string, object string) string {
	if len(keyPrefix) == 0 {
		return object
	}

	return fmt.Sprintf("%v/%v", keyPrefix, object)
}

func snapshotDir(diskID string, snapshotID string) string {
	if len(diskID) == 0 {
		diskID = "-"
	}

	return fmt.Sprintf("snapshots/%v/%v", diskID, snapshotID)
}
