//go:build darwin
// +build darwin

package portmanager

import (
	"golang.org/x/sys/unix"
)

func getEphemeralPortRange() (start, end int, err error) {
	startUint, err := unix.SysctlUint32("net.inet.ip.portrange.first")
	if err != nil {
		return portIANAEphemeralStart, portMax, nil
	}

	endUint, err := unix.SysctlUint32("net.inet.ip.portrange.last")
	if err != nil {
		return portIANAEphemeralStart, portMax, nil
	}

	return int(startUint), int(endUint), nil
}
