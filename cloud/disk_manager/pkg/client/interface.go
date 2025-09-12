package client

import (
	"context"

	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Client interface {
	// DiskService:
	CreateDisk(ctx context.Context, req *disk_manager.CreateDiskRequest) (*disk_manager.Operation, error)
	DeleteDisk(ctx context.Context, req *disk_manager.DeleteDiskRequest) (*disk_manager.Operation, error)
	ResizeDisk(ctx context.Context, req *disk_manager.ResizeDiskRequest) (*disk_manager.Operation, error)
	AlterDisk(ctx context.Context, req *disk_manager.AlterDiskRequest) (*disk_manager.Operation, error)
	AssignDisk(ctx context.Context, req *disk_manager.AssignDiskRequest) (*disk_manager.Operation, error)
	UnassignDisk(ctx context.Context, req *disk_manager.UnassignDiskRequest) (*disk_manager.Operation, error)
	DescribeDiskModel(ctx context.Context, req *disk_manager.DescribeDiskModelRequest) (*disk_manager.DiskModel, error)
	StatDisk(ctx context.Context, req *disk_manager.StatDiskRequest) (*disk_manager.DiskStats, error)
	MigrateDisk(ctx context.Context, req *disk_manager.MigrateDiskRequest) (*disk_manager.Operation, error)
	SendMigrationSignal(ctx context.Context, req *disk_manager.SendMigrationSignalRequest) error
	DescribeDisk(ctx context.Context, req *disk_manager.DescribeDiskRequest) (*disk_manager.DiskParams, error)
	ListDiskStates(ctx context.Context, req *disk_manager.ListDiskStatesRequest) (*disk_manager.ListDiskStatesResponse, error)

	// ImageService:
	CreateImage(ctx context.Context, req *disk_manager.CreateImageRequest) (*disk_manager.Operation, error)
	UpdateImage(ctx context.Context, req *disk_manager.UpdateImageRequest) (*disk_manager.Operation, error)
	DeleteImage(ctx context.Context, req *disk_manager.DeleteImageRequest) (*disk_manager.Operation, error)

	// OperationService:
	GetOperation(ctx context.Context, req *disk_manager.GetOperationRequest) (*disk_manager.Operation, error)
	CancelOperation(ctx context.Context, req *disk_manager.CancelOperationRequest) (*disk_manager.Operation, error)
	WaitOperation(ctx context.Context, operationID string, callback func(context.Context, *disk_manager.Operation) error) (*disk_manager.Operation, error)

	// PlacementGroupService:
	CreatePlacementGroup(ctx context.Context, req *disk_manager.CreatePlacementGroupRequest) (*disk_manager.Operation, error)
	DeletePlacementGroup(ctx context.Context, req *disk_manager.DeletePlacementGroupRequest) (*disk_manager.Operation, error)
	AlterPlacementGroupMembership(ctx context.Context, req *disk_manager.AlterPlacementGroupMembershipRequest) (*disk_manager.Operation, error)
	ListPlacementGroups(ctx context.Context, req *disk_manager.ListPlacementGroupsRequest) (*disk_manager.ListPlacementGroupsResponse, error)
	DescribePlacementGroup(ctx context.Context, req *disk_manager.DescribePlacementGroupRequest) (*disk_manager.PlacementGroup, error)

	// SnapshotService:
	CreateSnapshot(ctx context.Context, req *disk_manager.CreateSnapshotRequest) (*disk_manager.Operation, error)
	DeleteSnapshot(ctx context.Context, req *disk_manager.DeleteSnapshotRequest) (*disk_manager.Operation, error)

	// FilesystemService
	CreateFilesystem(ctx context.Context, req *disk_manager.CreateFilesystemRequest) (*disk_manager.Operation, error)
	DeleteFilesystem(ctx context.Context, req *disk_manager.DeleteFilesystemRequest) (*disk_manager.Operation, error)
	ResizeFilesystem(ctx context.Context, req *disk_manager.ResizeFilesystemRequest) (*disk_manager.Operation, error)
	DescribeFilesystemModel(ctx context.Context, req *disk_manager.DescribeFilesystemModelRequest) (*disk_manager.FilesystemModel, error)

	Close() error
}
