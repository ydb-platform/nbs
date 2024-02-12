package common

import "fmt"

////////////////////////////////////////////////////////////////////////////////

func GetProxyOverlayDiskID(diskID string, ID string) string {
	return fmt.Sprintf("proxy_%v_%v", diskID, ID)
}
