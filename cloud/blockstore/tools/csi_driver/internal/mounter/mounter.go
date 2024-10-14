package mounter

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"k8s.io/mount-utils"
)

////////////////////////////////////////////////////////////////////////////////

type mounter struct {
	mnt mount.Interface
}

func NewMounter() Interface {
	return &mounter{
		mnt: mount.New(""),
	}
}

func (m *mounter) Mount(source string, target string, fsType string, options []string) error {
	return m.mnt.Mount(source, target, fsType, options)
}

func (m *mounter) IsMountPoint(file string) (bool, error) {
	return m.mnt.IsMountPoint(file)
}

func (m *mounter) CleanupMountPoint(target string) error {
	return mount.CleanupMountPoint(target, m.mnt, true)
}

func (m *mounter) IsFilesystemExisted(device string) (bool, error) {
	if _, err := exec.LookPath("blkid"); err != nil {
		return false, fmt.Errorf("failed to find 'blkid' tool: %w", err)
	}

	if _, err := exec.LookPath("blockdev"); err != nil {
		return false, fmt.Errorf("failed to find 'blockdev' tool: %w", err)
	}

	if _, err := os.Stat(device); os.IsNotExist(err) {
		return false, fmt.Errorf("failed to find device %q: %w", device, err)
	}

	out, err := exec.Command("blockdev", "--getsize64", device).CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("failed to get size of device %q: %w", device, err)
	}

	deviceSize, err := strconv.ParseUint(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		return false, fmt.Errorf("failed to convert %q to number: %w", out, err)
	}

	if deviceSize == 0 {
		return false, fmt.Errorf("size of device %q is empty. blockdev output is %q",
			device, out)
	}

	out, err = exec.Command("blkid", device).CombinedOutput()
	return err == nil && string(out) != "", nil
}

func (m *mounter) MakeFilesystem(device string, fsType string) ([]byte, error) {
	options := []string{"-t", fsType}
	if fsType == "ext4" {
		options = append(options, "-E", "nodiscard")
	}
	if fsType == "xfs" {
		options = append(options, "-K")
	}

	options = append(options, device)
	return exec.Command("mkfs", options...).CombinedOutput()
}
