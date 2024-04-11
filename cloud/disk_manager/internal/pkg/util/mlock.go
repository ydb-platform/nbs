package util

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type memRange struct {
	start uintptr
	end   uintptr
}

type memItem struct {
	memoryRange memRange
	permissions string
	offset      uintptr
	device      string
	inode       uint64
	pathname    string
}

func parseMemRange(line string) (*memItem, error) {
	var item memItem
	n, err := fmt.Sscanf(
		line,
		"%x-%x %s %x %s %d %s",
		&item.memoryRange.start,
		&item.memoryRange.end,
		&item.permissions,
		&item.offset,
		&item.device,
		&item.inode,
		&item.pathname,
	)
	if err != nil && (err != io.EOF || n < 6) {
		return nil, err
	}
	return &item, nil
}

func mlock(memoryRange memRange) error {
	len := memoryRange.end - memoryRange.start
	_, _, errno := syscall.Syscall(syscall.SYS_MLOCK, memoryRange.start, len, 0)
	if errno != 0 {
		return errors.NewNonRetriableErrorf("mlock syscall failed with errno %d", errno)
	}
	return nil
}

func shouldLockRange(item *memItem) bool {
	return item.inode != 0 && item.permissions[0] == 'r'
}

func LockBinary() error {
	maps, err := os.Open("/proc/self/maps")
	if err != nil {
		return err
	}
	defer maps.Close()

	scanner := bufio.NewScanner(maps)
	for scanner.Scan() {
		item, err := parseMemRange(scanner.Text())
		if err != nil {
			return err
		}

		if shouldLockRange(item) {
			err = mlock(item.memoryRange)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
