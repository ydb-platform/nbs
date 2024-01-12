package storage

import (
	"context"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"path"
	"strings"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	task_errors "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/persistence"
	error_codes "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
)

////////////////////////////////////////////////////////////////////////////////

func (s *legacyStorage) readChunkMap(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	milestoneChunkIndex uint32,
	inflightQueue *common.InflightQueue,
) (<-chan ChunkMapEntry, <-chan error) {

	entries := make(chan ChunkMapEntry)
	errors := make(chan error, 1)

	snapshotInfo, err := s.readSnapshotInfo(ctx, session, snapshotID)
	if err != nil {
		errors <- err
		close(entries)
		close(errors)
		return entries, errors
	}

	res, err := session.StreamReadTable(
		ctx,
		path.Join(s.tablesPath, "snapshotchunks"),
		persistence.ReadOrdered(),
		persistence.ReadColumn("chunkoffset"),
		persistence.ReadColumn("chunkid"),
		persistence.ReadGreaterOrEqual(persistence.TupleValue(
			persistence.OptionalValue(persistence.StringValue([]byte(snapshotInfo.tree))),
			persistence.OptionalValue(persistence.StringValue([]byte(snapshotID))),
			persistence.OptionalValue(persistence.Int64Value(int64(milestoneChunkIndex))),
		)),
		persistence.ReadLessOrEqual(persistence.TupleValue(
			persistence.OptionalValue(persistence.StringValue([]byte(snapshotInfo.tree))),
			persistence.OptionalValue(persistence.StringValue([]byte(snapshotID))),
			persistence.OptionalValue(persistence.Int64Value(math.MaxInt64)),
		)),
	)
	if err != nil {
		errors <- err
		close(entries)
		close(errors)
		return entries, errors
	}

	go func() {
		defer res.Close()
		defer close(entries)
		defer close(errors)

		defer func() {
			if r := recover(); r != nil {
				errors <- task_errors.NewPanicError(r)
			}
		}()

		for res.NextResultSet(ctx) {
			for res.NextRow() {
				var (
					chunkOffset int64
					chunkID     []byte
				)
				err = res.ScanNamed(
					persistence.OptionalWithDefault("chunkoffset", &chunkOffset),
					persistence.OptionalWithDefault("chunkid", &chunkID),
				)
				if err != nil {
					errors <- err
					return
				}

				entry := ChunkMapEntry{
					ChunkIndex: uint32(chunkOffset / snapshotInfo.chunkSize),
					ChunkID:    string(chunkID),
				}

				if inflightQueue != nil {
					_, err := inflightQueue.Add(ctx, entry.ChunkIndex)
					if err != nil {
						errors <- err
						return
					}
				}

				select {
				case entries <- entry:
				case <-ctx.Done():
					errors <- ctx.Err()
					return
				}
			}
		}

		// NOTE: always check scan query result after iteration.
		err = res.Err()
		if err != nil {
			errors <- task_errors.NewRetriableError(err)
		}
	}()

	return entries, errors
}

////////////////////////////////////////////////////////////////////////////////

func (s *legacyStorage) readChunk(
	ctx context.Context,
	session *persistence.Session,
	chunk *dataplane_common.Chunk,
) (err error) {

	defer s.metrics.StatOperation("legacy/readChunk")(&err)

	if len(chunk.ID) == 0 {
		return task_errors.NewNonRetriableErrorf("chunkID should not be empty")
	}

	chunkInfo, err := s.readChunkInfo(ctx, session, chunk.ID)
	if err != nil {
		return err
	}

	if chunkInfo.zero {
		chunk.Zero = true
		return nil
	}

	data, err := s.readChunkData(ctx, session, chunk.ID)
	if err != nil {
		return err
	}

	err = compressor.Decompress(chunkInfo.format, data, chunk.Data, s.metrics)
	if err != nil {
		return err
	}

	if len(chunk.Data) != int(chunkInfo.size) {
		return task_errors.NewNonRetriableErrorf(
			"chunk size mismatch: expected %v, actual %v",
			chunkInfo.size,
			len(chunk.Data),
		)
	}

	checksumType := getChecksumType(chunkInfo.sum)
	checksum := getChecksum(chunk.Data, checksumType)

	if checksum != chunkInfo.sum {
		return task_errors.NewNonRetriableErrorf(
			"chunk checksum mismatch: expected %v, actual %v",
			chunkInfo.sum,
			checksum,
		)
	}

	return nil
}

func (s *legacyStorage) checkSnapshotReady(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) (meta SnapshotMeta, err error) {

	snapshotInfo, err := s.readSnapshotInfo(ctx, session, snapshotID)
	if err != nil {
		return SnapshotMeta{}, err
	}

	if snapshotInfo.state != "ready" {
		return SnapshotMeta{}, task_errors.NewSilentNonRetriableErrorf(
			"snapshot with id %v is not ready",
			snapshotID,
		)
	}

	return SnapshotMeta{
		Size:        uint64(snapshotInfo.size),
		StorageSize: uint64(snapshotInfo.realSize),
		ChunkCount:  uint32(snapshotInfo.size / snapshotInfo.chunkSize),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type snapshotInfo struct {
	base      string
	tree      string
	chunkSize int64
	size      int64
	realSize  int64
	state     string
}

func (s *legacyStorage) readSnapshotInfo(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) (snapshotInfo, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as String;

		select *
		from snapshotsext
		where id = $snapshot_id;
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.StringValue([]byte(snapshotID))),
	)
	if err != nil {
		return snapshotInfo{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return snapshotInfo{}, task_errors.NewDetailedError(
			task_errors.NewNonRetriableErrorf(
				"snapshot with id %v is not found",
				snapshotID,
			),
			&task_errors.ErrorDetails{
				Code:     error_codes.BadSource,
				Message:  "snapshot or image not found",
				Internal: false,
			},
		)
	}

	var (
		base      []byte
		tree      []byte
		chunkSize int64
		size      int64
		realSize  int64
		state     []byte
	)
	err = res.ScanNamed(
		persistence.OptionalWithDefault("base", &base),
		persistence.OptionalWithDefault("tree", &tree),
		persistence.OptionalWithDefault("chunksize", &chunkSize),
		persistence.OptionalWithDefault("size", &size),
		persistence.OptionalWithDefault("realsize", &realSize),
		persistence.OptionalWithDefault("state", &state),
	)
	if err != nil {
		return snapshotInfo{}, err
	}

	return snapshotInfo{
		base:      string(base),
		tree:      string(tree),
		chunkSize: chunkSize,
		size:      size,
		realSize:  realSize,
		state:     string(state),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type chunkInfo struct {
	sum    string
	format string
	size   int64
	refcnt int64
	zero   bool
}

func (s *legacyStorage) readChunkInfo(
	ctx context.Context,
	session *persistence.Session,
	chunkID string,
) (chunkInfo, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $chunk_id as String;

		select *
		from chunks
		where id = $chunk_id;
	`, s.tablesPath),
		persistence.ValueParam("$chunk_id", persistence.StringValue([]byte(chunkID))),
	)
	if err != nil {
		return chunkInfo{}, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return chunkInfo{}, task_errors.NewNonRetriableErrorf(
			"chunk not found: %v",
			chunkID,
		)
	}
	var (
		sum    []byte
		format []byte
		size   int64
		refcnt int64
		zero   bool
	)
	err = res.ScanNamed(
		persistence.OptionalWithDefault("sum", &sum),
		persistence.OptionalWithDefault("format", &format),
		persistence.OptionalWithDefault("size", &size),
		persistence.OptionalWithDefault("refcnt", &refcnt),
		persistence.OptionalWithDefault("zero", &zero),
	)
	if err != nil {
		return chunkInfo{}, err
	}

	return chunkInfo{
		sum:    string(sum),
		format: string(format),
		size:   size,
		refcnt: refcnt,
		zero:   zero,
	}, nil
}

func (s *legacyStorage) readChunkData(
	ctx context.Context,
	session *persistence.Session,
	chunkID string,
) ([]byte, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $chunk_id as String;

		select *
		from blobs_on_hdd
		where id = $chunk_id;
	`, s.tablesPath),
		persistence.ValueParam("$chunk_id", persistence.StringValue([]byte(chunkID))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return nil, task_errors.NewNonRetriableErrorf(
			"chunk not found: %v",
			chunkID,
		)
	}
	var data []byte
	err = res.ScanNamed(
		persistence.OptionalWithDefault("data", &data),
	)
	if err != nil {
		return nil, err
	}

	return data, nil
}

////////////////////////////////////////////////////////////////////////////////

func getChecksumType(checksum string) string {
	parts := strings.SplitN(checksum, ":", 2)
	if len(parts) == 2 {
		return parts[0]
	}
	return ""
}

func getChecksum(data []byte, checksumType string) string {
	var hasher hash.Hash
	if checksumType == "sha512" {
		hasher = sha512.New()
	} else {
		return ""
	}

	_, err := hasher.Write(data)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%v:%v", checksumType, hex.EncodeToString(hasher.Sum(nil)))
}
