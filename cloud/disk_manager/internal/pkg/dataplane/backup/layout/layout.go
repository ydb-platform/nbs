package layout

import (
	"fmt"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////

// Objects are keys relative to the slave prefix; Key adds the prefix.

func ChunkObject(chunkID string) string {
	return fmt.Sprintf("chunks/%v", chunkID)
}

func MetaObject(diskID string, snapshotID string) string {
	return fmt.Sprintf("%v/meta.json", snapshotDir(diskID, snapshotID))
}

func MapObject(diskID string, snapshotID string) string {
	return fmt.Sprintf("%v/map.bin", snapshotDir(diskID, snapshotID))
}

func Key(prefix string, object string) string {
	prefix = strings.TrimSuffix(prefix, "/")
	if len(prefix) == 0 {
		return object
	}

	return fmt.Sprintf("%v/%v", prefix, object)
}

////////////////////////////////////////////////////////////////////////////////

func snapshotDir(diskID string, snapshotID string) string {
	if len(diskID) == 0 {
		diskID = "-"
	}

	return fmt.Sprintf("snapshots/%v/%v", diskID, snapshotID)
}
