//go:build !linux && !darwin
// +build !linux,!darwin

package portmanager

import (
	"os"
)

func getEphemeralPortRange() (start, end int, err error) {
	return portIANAEphemeralStart, portMax, nil
}

// tryLockFile is not implemented on windows right now.
//
// Implementation should match python version from.
//
// https://a.yandex-team.ru/arc/trunk/arcadia/library/python/filelock/__init__.py?rev=r8352156#L66-103
func tryLockFile(file *os.File) error {
	return nil
}

func unlockFile(file *os.File) error {
	return nil
}
