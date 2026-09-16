package backup

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////

func ChunkKey(keyPrefix string, chunkID string) string {
	return key(keyPrefix, fmt.Sprintf("chunks/%v", chunkID))
}

func MetaKey(keyPrefix string, diskID string, snapshotID string) string {
	if len(diskID) == 0 {
		diskID = "-"
	}

	return key(keyPrefix, fmt.Sprintf("snapshots/%v/%v/meta.json", diskID, snapshotID))
}

func ChunkMapKey(keyPrefix string, snapshotID string) string {
	return key(keyPrefix, fmt.Sprintf("chunk_maps/%v", snapshotID))
}

////////////////////////////////////////////////////////////////////////////////

func key(keyPrefix string, object string) string {
	if len(keyPrefix) == 0 {
		return object
	}

	return fmt.Sprintf("%v/%v", keyPrefix, object)
}
