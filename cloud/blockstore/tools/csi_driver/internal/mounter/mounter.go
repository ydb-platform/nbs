package mounter

import (
	"fmt"
	"os"
	"os/exec"

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
		return false, fmt.Errorf("failed to find 'blkid' tool: %v", err)
	}

	if _, err := os.Stat(device); os.IsNotExist(err) {
		return false, fmt.Errorf("failed to find device %q: %v", device, err)
	}

	out, err := exec.Command("blkid", device).CombinedOutput()
	return err == nil && string(out) != "", nil
}

func (m *mounter) MakeFilesystem(device string, fsType string) error {
	options := []string{"-t", fsType}
	if fsType == "ext4" {
		options = append(options, "-E", "nodiscard")
	}
	if fsType == "xfs" {
		options = append(options, "-K")
	}

	options = append(options, device)
	out, err := exec.Command("mkfs", options...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to make filesystem: %v, output %q", err, out)
	}

	return nil
}
