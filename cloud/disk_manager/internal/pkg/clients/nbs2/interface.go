package nbs2

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type CreatePartitionParams struct {
	DiskID          string
	BlockSize       uint32
	BlocksCount     uint64
	StoragePoolName string
}

type Client interface {
	CreatePartition(ctx context.Context, params CreatePartitionParams) (tabletID string, err error)
	DeletePartition(ctx context.Context, tabletID string) error
	ZoneID() string
}

type Factory interface {
	GetClient(ctx context.Context, zoneID string) (Client, error)
}
