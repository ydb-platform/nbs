package util

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

type memoryRange struct {
	start uintptr
	end   uintptr
}

type procMapsItem struct {
	memRange    memoryRange
	permissions string
	offset      uintptr
	device      string
	inode       uint64
	pathname    string
}

func parseProcMapsLine(line string) (*procMapsItem, error) {
	var item procMapsItem
	n, err := fmt.Sscanf(
		line,
		"%x-%x %s %x %s %d %s",
		&item.memRange.start,
		&item.memRange.end,
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

func mlock(memoryRange memoryRange) error {
	len := memoryRange.end - memoryRange.start
	_, _, errno := syscall.Syscall(syscall.SYS_MLOCK, memoryRange.start, len, 0)
	if errno != 0 {
		return errors.NewNonRetriableErrorf("mlock syscall failed with errno %d", errno)
	}
	return nil
}

func shouldLockRange(item *procMapsItem) bool {
	return item.inode != 0 && item.permissions[0] == 'r'
}

func LockProcessMemory() error {
	maps, err := os.Open("/proc/self/maps")
	if err != nil {
		return err
	}
	defer maps.Close()

	scanner := bufio.NewScanner(maps)
	for scanner.Scan() {
		item, err := parseProcMapsLine(scanner.Text())
		if err != nil {
			return err
		}

		if shouldLockRange(item) {
			err = mlock(item.memRange)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
