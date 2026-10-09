package chunks

import (
	"bytes"
	"context"
	"hash/crc32"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	sm "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestBackupChunkBlobPreservesPayloadAndMetadata(t *testing.T) {
	for _, c := range testCases() {
		for _, compression := range []string{"", "gzip"} {
			t.Run(c.name+"/"+compression, func(t *testing.T) {
				ctx, db, s3, config := setupEnvironment(t)
				defer db.Close(ctx)
				storage := newStorage(db, s3, config, c.useS3)
				data := bytes.Repeat([]byte("independent backup oracle 0123456789"), 4096)
				require.NoError(t, storage.WriteChunk(ctx, "owner", common.Chunk{ID: "owned", Data: data, Compression: compression}))
				blob, err := storage.ReadChunkBlob(ctx, "owned")
				require.NoError(t, err)
				require.Equal(t, compression, blob.Compression)
				require.Equal(t, crc32.ChecksumIEEE(data), blob.Checksum)
				decoded := make([]byte, len(data))
				require.NoError(t, compressor.Decompress(blob.Compression, blob.Data, decoded, sm.New(metrics.NewEmptyRegistry(), "test")))
				require.Equal(t, data, decoded)
				// Exported metadata must survive a physical copy into another object.
				require.NoError(t, s3.PutObject(ctx, config.GetS3Bucket(), test.NewS3Key(config, "copy"), NewS3Object(blob)))
				copied, err := newStorage(db, s3, config, true).ReadChunkBlob(ctx, "copy")
				require.NoError(t, err)
				require.Equal(t, blob, copied)
				_, err = storage.ReadChunkBlob(ctx, "absent")
				require.Error(t, err)
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				_, err = storage.ReadChunkBlob(cancelled, "owned")
				require.Error(t, err)
				if c.useS3 {
					for _, metadata := range []map[string]*string{nil, {"Checksum": proto.String("corrupt")}} {
						require.NoError(t, s3.PutObject(ctx, config.GetS3Bucket(), test.NewS3Key(config, "corrupt"), persistence.S3Object{Data: blob.Data, Metadata: metadata}))
						_, err = storage.ReadChunkBlob(ctx, "corrupt")
						require.Error(t, err)
					}
				}
			})
		}
	}
}
