package nfs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type CreateFilesystemParams struct {
	FolderID    string
	CloudID     string
	BlockSize   uint32
	BlocksCount uint64
	Kind        types.FilesystemKind
	ShardCount  uint32
}

type FilesystemPerformanceProfile struct {
	MaxReadBandwidth  uint64
	MaxReadIops       uint32
	MaxWriteBandwidth uint64
	MaxWriteIops      uint32
}

type FilesystemModel struct {
	BlockSize          uint32
	BlocksCount        uint64
	ChannelsCount      uint32
	Kind               types.FilesystemKind
	PerformanceProfile FilesystemPerformanceProfile
}

////////////////////////////////////////////////////////////////////////////////

type Session interface {
	ListNodes(
		ctx context.Context,
		parentNodeID uint64,
		cookie string,
		maxBytes uint32,
		unsafe bool,
	) ([]Node, string, error)

	CreateCheckpoint(
		ctx context.Context,
		filesystemID string,
		checkpointID string,
		nodeID uint64,
	) error

	CreateNode(
		ctx context.Context,
		node Node,
	) (uint64, error)

	CreateNodeIdempotent(
		ctx context.Context,
		node Node,
	) (uint64, error)

	ReadLink(
		ctx context.Context,
		nodeID uint64,
	) ([]byte, error)

	GetNodeAttr(
		ctx context.Context,
		parentNodeID uint64,
		name string,
	) (Node, error)

	UnlinkNode(
		ctx context.Context,
		parentNodeID uint64,
		name string,
		unlinkDirectory bool,
	) error

	Close(ctx context.Context) error
}

////////////////////////////////////////////////////////////////////////////////

type Client interface {
	Close() error

	ZoneID() string

	Create(
		ctx context.Context,
		filesystemID string,
		params CreateFilesystemParams,
	) error

	Delete(ctx context.Context, filesystemID string, force bool) error

	Resize(ctx context.Context, filesystemID string, size uint64) error

	EnableDirectoryCreationInShards(
		ctx context.Context,
		filesystemID string,
		shardCount uint32,
	) error

	DescribeModel(
		ctx context.Context,
		blocksCount uint64,
		blockSize uint32,
		kind types.FilesystemKind,
	) (FilesystemModel, error)

	DestroyCheckpoint(
		ctx context.Context,
		filesystemID string,
		checkpointID string,
	) error

	CreateSession(
		ctx context.Context,
		fileSystemID string,
		checkpointID string,
		readonly bool,
	) (Session, error)
}

////////////////////////////////////////////////////////////////////////////////

type Factory interface {
	NewClient(ctx context.Context, zoneID string) (Client, error)

	// Returns client from default zone. Use it carefully.
	NewClientFromDefaultZone(ctx context.Context) (Client, error)
}
