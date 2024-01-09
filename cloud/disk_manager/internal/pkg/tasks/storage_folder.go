package tasks

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/headers"
	grpc_metadata "google.golang.org/grpc/metadata"
)

////////////////////////////////////////////////////////////////////////////////

// TODO: NBS-1858: Avoid collisions with user defined keys (maybe do some kind
// of user keys filtering).
const storageFolderKey = "storage-folder"

func getStorageFolder(ctx context.Context) string {
	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := metadata.Get(storageFolderKey)
	if len(vals) == 0 {
		return ""
	}

	return vals[0]
}

func setStorageFolder(
	ctx context.Context,
	storageFolder string,
) context.Context {

	return headers.Append(
		ctx,
		map[string]string{storageFolderKey: storageFolder},
	)
}
