package mounter

import (
	"context"

	"github.com/stretchr/testify/mock"
)

////////////////////////////////////////////////////////////////////////////////

type Mock struct {
	mock.Mock
}

func (c *Mock) Mount(
	source string,
	target string,
	fsType string,
	options []string) error {

	args := c.Called(source, target, fsType, options)
	return args.Error(0)
}

func (c *Mock) IsMountPoint(file string) (bool, error) {
	args := c.Called(file)
	return args.Get(0).(bool), args.Error(1)
}

func (c *Mock) CleanupMountPoint(target string) error {
	args := c.Called(target)
	return args.Error(0)
}

func (c *Mock) HasBlockDevice(ctx context.Context, device string) (bool, error) {
	args := c.Called(device)
	return args.Get(0).(bool), args.Error(1)
}

func (c *Mock) IsFilesystemExisted(ctx context.Context, device string) (bool, error) {
	args := c.Called(device)
	return args.Get(0).(bool), args.Error(1)
}

func (c *Mock) IsFilesystemRemountedAsReadonly(mountPoint string) (bool, error) {
	args := c.Called(mountPoint)
	return args.Get(0).(bool), args.Error(1)
}

func (c *Mock) MakeFilesystem(device string, fsType string) ([]byte, error) {
	args := c.Called(device, fsType)
	return args.Get(0).([]byte), args.Error(1)
}

func (c *Mock) NeedResize(devicePath string, deviceMountPath string) (bool, error) {
	args := c.Called(devicePath, deviceMountPath)
	return args.Get(0).(bool), args.Error(1)
}

func (c *Mock) Resize(devicePath string, deviceMountPath string) (bool, error) {
	args := c.Called(devicePath, deviceMountPath)
	return args.Get(0).(bool), args.Error(1)
}

func (c *Mock) FormatAndMount(source string, target string, fsType string, options []string) error {
	args := c.Called(source, target, fsType, options)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewMock() *Mock {
	return &Mock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that Mock implements Interface.
func assertMounterMockIsMounter(arg *Mock) Interface {
	return arg
}
