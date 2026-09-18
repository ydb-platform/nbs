package backup

import (
	"fmt"
)

////////////////////////////////////////////////////////////////////////////////

func SnapshotMetaKey(
	keyPrefix string,
	diskID string,
	snapshotID string,
) string {

	return key(
		keyPrefix,
		fmt.Sprintf("snapshots/%v/%v/meta.json", diskID, snapshotID),
	)
}

func ImageMetaKey(keyPrefix string, imageID string) string {
	return key(keyPrefix, fmt.Sprintf("images/%v/meta.json", imageID))
}

func ChunkKey(keyPrefix string, chunkID string) string {
	return key(keyPrefix, fmt.Sprintf("chunks/%v", chunkID))
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
