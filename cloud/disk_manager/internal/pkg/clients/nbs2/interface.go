package nbs2

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type CreateDiskParams struct {
	ID              string
	BlocksCount     uint64
	BlockSize       uint32
	StoragePoolName string
}

type Client interface {
	Create(ctx context.Context, params CreateDiskParams) error
	Delete(ctx context.Context, diskID string) error
}

type Factory interface {
	GetClient(ctx context.Context, zoneID string) (Client, error)
}
