package common

import "fmt"

////////////////////////////////////////////////////////////////////////////////

func GetProxyOverlayDiskID(prefix, diskID, snapshotID string) string {
	return fmt.Sprintf("%v%v_%v", prefix, diskID, snapshotID)
}
