//go:build linux || darwin
// +build linux darwin

package portmanager

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func tryLockFile(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		return fmt.Errorf("flock(%q, LOCK_EX|LOCK_NB): %v", file.Name(), err)
	}
	return nil
}

func unlockFile(file *os.File) error {
	err := unix.Flock(int(file.Fd()), unix.LOCK_UN)
	if err != nil {
		return fmt.Errorf("flock(%q, LOCK_UN): %v", file.Name(), err)
	}
	return nil
}
