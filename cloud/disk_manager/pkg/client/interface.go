package client

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/api/operation"
	"github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type Client interface {
	// DiskService:
	CreateDisk(ctx context.Context, req *disk_manager.CreateDiskRequest) (*operation.Operation, error)
	DeleteDisk(ctx context.Context, req *disk_manager.DeleteDiskRequest) (*operation.Operation, error)
	ResizeDisk(ctx context.Context, req *disk_manager.ResizeDiskRequest) (*operation.Operation, error)
	AlterDisk(ctx context.Context, req *disk_manager.AlterDiskRequest) (*operation.Operation, error)
	AssignDisk(ctx context.Context, req *disk_manager.AssignDiskRequest) (*operation.Operation, error)
	UnassignDisk(ctx context.Context, req *disk_manager.UnassignDiskRequest) (*operation.Operation, error)
	DescribeDiskModel(ctx context.Context, req *disk_manager.DescribeDiskModelRequest) (*disk_manager.DiskModel, error)
	StatDisk(ctx context.Context, req *disk_manager.StatDiskRequest) (*disk_manager.DiskStats, error)
	MigrateDisk(ctx context.Context, req *disk_manager.MigrateDiskRequest) (*operation.Operation, error)
	SendMigrationSignal(ctx context.Context, req *disk_manager.SendMigrationSignalRequest) error
	DescribeDisk(ctx context.Context, req *disk_manager.DescribeDiskRequest) (*disk_manager.DiskParams, error)

	// ImageService:
	CreateImage(ctx context.Context, req *disk_manager.CreateImageRequest) (*operation.Operation, error)
	UpdateImage(ctx context.Context, req *disk_manager.UpdateImageRequest) (*operation.Operation, error)
	DeleteImage(ctx context.Context, req *disk_manager.DeleteImageRequest) (*operation.Operation, error)

	// OperationService:
	GetOperation(ctx context.Context, req *disk_manager.GetOperationRequest) (*operation.Operation, error)
	CancelOperation(ctx context.Context, req *disk_manager.CancelOperationRequest) (*operation.Operation, error)
	WaitOperation(ctx context.Context, operationID string, callback func(context.Context, *operation.Operation) error) (*operation.Operation, error)

	// PlacementGroupService:
	CreatePlacementGroup(ctx context.Context, req *disk_manager.CreatePlacementGroupRequest) (*operation.Operation, error)
	DeletePlacementGroup(ctx context.Context, req *disk_manager.DeletePlacementGroupRequest) (*operation.Operation, error)
	AlterPlacementGroupMembership(ctx context.Context, req *disk_manager.AlterPlacementGroupMembershipRequest) (*operation.Operation, error)
	ListPlacementGroups(ctx context.Context, req *disk_manager.ListPlacementGroupsRequest) (*disk_manager.ListPlacementGroupsResponse, error)
	DescribePlacementGroup(ctx context.Context, req *disk_manager.DescribePlacementGroupRequest) (*disk_manager.PlacementGroup, error)

	// SnapshotService:
	CreateSnapshot(ctx context.Context, req *disk_manager.CreateSnapshotRequest) (*operation.Operation, error)
	DeleteSnapshot(ctx context.Context, req *disk_manager.DeleteSnapshotRequest) (*operation.Operation, error)

	// FilesystemService
	CreateFilesystem(ctx context.Context, req *disk_manager.CreateFilesystemRequest) (*operation.Operation, error)
	DeleteFilesystem(ctx context.Context, req *disk_manager.DeleteFilesystemRequest) (*operation.Operation, error)
	ResizeFilesystem(ctx context.Context, req *disk_manager.ResizeFilesystemRequest) (*operation.Operation, error)
	DescribeFilesystemModel(ctx context.Context, req *disk_manager.DescribeFilesystemModelRequest) (*disk_manager.FilesystemModel, error)

	Close() error
}
