package backup

import (
	"fmt"
)

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
