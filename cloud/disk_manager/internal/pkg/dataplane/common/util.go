package common

import "fmt"

////////////////////////////////////////////////////////////////////////////////

func GetProxyOverlayDiskID(prefix string, diskID string, snapshotID string) string {
	return fmt.Sprintf("%v%v_%v", prefix, diskID, snapshotID)
}
