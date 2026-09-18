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
