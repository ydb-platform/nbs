package backup

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////

// The chunk map of a snapshot keeps the disk of the snapshot in this metadata:
// the meta lies under the disk, the chunk map under the snapshot ID alone.
const DiskIDMetadataKey = "Disk-Id"

////////////////////////////////////////////////////////////////////////////////

func SnapshotMetaKey(diskID string, snapshotID string) string {
	return fmt.Sprintf("snapshots/%v/%v/meta.json", diskID, snapshotID)
}

func ImageMetaKey(imageID string) string {
	return fmt.Sprintf("images/%v/meta.json", imageID)
}

func ChunkKey(chunkID string) string {
	return fmt.Sprintf("chunks/%v", chunkID)
}

func ChunkMapKey(snapshotID string) string {
	return fmt.Sprintf("chunk_maps/%v", snapshotID)
}
