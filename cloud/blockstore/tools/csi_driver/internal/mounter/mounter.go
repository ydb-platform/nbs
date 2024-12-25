package mounter

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"

	"k8s.io/mount-utils"
	executils "k8s.io/utils/exec"
)

////////////////////////////////////////////////////////////////////////////////

type mounter struct {
	mnt  mount.Interface
	exec executils.Interface
}

func NewMounter() Interface {
	return &mounter{
		mnt:  mount.New(""),
		exec: executils.New(),
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

func (m *mounter) HasBlockDevice(device string) (bool, error) {
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
		return false, fmt.Errorf(
			"size of device %q is empty. blockdev output: %q", device, out)
	}

	return true, nil
}

func (m *mounter) IsFilesystemExisted(device string) (bool, error) {
	hasBlockDevice, err := m.HasBlockDevice(device)
	if !hasBlockDevice {
		return false, fmt.Errorf("failed to check filesystem: %w", err)
	}

	if _, err = exec.LookPath("blkid"); err != nil {
		return false, fmt.Errorf("failed to find 'blkid' tool: %w", err)
	}

	out, err := exec.Command("blkid", device).CombinedOutput()
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

func (m *mounter) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	return mount.NewResizeFs(m.exec).NeedResize(devicePath, deviceMountPath)
}

func (m *mounter) Resize(devicePath string, deviceMountPath string) (bool, error) {
	return mount.NewResizeFs(m.exec).Resize(devicePath, deviceMountPath)
}

func (m *mounter) FormatAndMount(source string, target string, fsType string, options []string) error {
	formatOptions := []string{"-t", fsType}
	if fsType == "ext4" {
		formatOptions = append(formatOptions, "-E", "nodiscard")
	}
	if fsType == "xfs" {
		formatOptions = append(formatOptions, "-K")
	}

	safeFormatAndMount := mount.NewSafeFormatAndMount(m.mnt, m.exec)
	return safeFormatAndMount.FormatAndMountSensitiveWithFormatOptions(
		source,
		target,
		fsType,
		options,
		nil,
		formatOptions)
}
