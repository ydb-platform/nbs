package backup

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////

// Layout of the backup bucket:
//
//	chunks/<chunk_id>                            chunk object, copied as is
//	snapshots/<disk_id>/<snapshot_id>/meta.json  snapshot meta, written first
//	snapshots/<disk_id>/<snapshot_id>/map.bin    chunk map, written last

func chunkKey(keyPrefix string, chunkID string) string {
	return key(keyPrefix, fmt.Sprintf("chunks/%v", chunkID))
}

func metaKey(keyPrefix string, diskID string, snapshotID string) string {
	return key(keyPrefix, snapshotDir(diskID, snapshotID)+"/meta.json")
}

func chunkMapKey(keyPrefix string, diskID string, snapshotID string) string {
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
