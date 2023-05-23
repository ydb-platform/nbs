//go:build linux
// +build linux

package portmanager

import (
	"fmt"
	"os"
)

func getEphemeralPortRange() (start, end int, err error) {
	localPortRange, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		if !os.IsNotExist(err) {
			return 0, 0, err

		}

		return portIANAEphemeralStart, portMax, nil
	}

	_, err = fmt.Sscanf(string(localPortRange), "%d\t%d", &start, &end)
	return
}
