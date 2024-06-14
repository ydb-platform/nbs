package mounter

////////////////////////////////////////////////////////////////////////////////

type Interface interface {
	Mount(source string, target string, fsType string, options []string) error
	IsMountPoint(file string) (bool, error)
	CleanupMountPoint(target string) error

	IsFilesystemExisted(device string) (bool, error)
	MakeFilesystem(device string, fsType string) ([]byte, error)
}
