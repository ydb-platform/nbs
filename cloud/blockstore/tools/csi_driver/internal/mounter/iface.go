package mounter

import "context"

////////////////////////////////////////////////////////////////////////////////

type Interface interface {
	Mount(source string, target string, fsType string, options []string) error
	FormatAndMount(source string, target string, fsType string, options []string) error

	IsMountPoint(file string) (bool, error)
	CleanupMountPoint(target string) error

	HasBlockDevice(context context.Context, device string) (bool, error)

	IsFilesystemExisted(ctx context.Context, device string) (bool, error)
	IsFilesystemRemountedAsReadonly(mountPoint string) (bool, error)
	MakeFilesystem(device string, fsType string) ([]byte, error)

	NeedResize(devicePath string, deviceMountPath string) (bool, error)
	Resize(devicePath, deviceMountPath string) (bool, error)
}
