package exporter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type s3PartBuffer struct {
	data []byte
}

type s3PartUpload struct {
	buffer   *s3PartBuffer
	part     S3ManifestPart
	started  time.Time
	ready    time.Time
	finished time.Time
}

type s3ReadSource struct {
	Source
	slots      *semaphore.Weighted
	progress   *s3ExportProgress
	maxRetries int
}

func newS3ReadSource(
	source Source,
	readWorkerCount int,
	progress *s3ExportProgress,
	maxRetries int,
) Source {

	reader := &s3ReadSource{
		Source:     source,
		slots:      semaphore.NewWeighted(int64(readWorkerCount)),
		progress:   progress,
		maxRetries: maxRetries,
	}

	if rangeReader, ok := source.(storage.ChunkMapRangeReader); ok {
		return &s3RangeReadSource{
			s3ReadSource:        reader,
			ChunkMapRangeReader: rangeReader,
		}
	}

	return reader
}

// Preserve the optional range capability through the shared read limiter.
type s3RangeReadSource struct {
	*s3ReadSource
	storage.ChunkMapRangeReader
}

func (s *s3ReadSource) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	waitStarted := time.Now()
	s.progress.metrics.addActive(s3StageReadWait, 1)
	err := s.slots.Acquire(ctx, 1)
	s.progress.metrics.addActive(s3StageReadWait, -1)
	s.progress.metrics.recordStage(ctx, s3StageReadWait, waitStarted, err)
	if err != nil {
		return err
	}
	defer s.slots.Release(1)

	s.progress.startRead()
	defer s.progress.finishRead()
	started := time.Now()
	err = retryS3Call(ctx, s.maxRetries, func() error {
		return s.Source.ReadChunk(ctx, chunk)
	})
	s.progress.metrics.recordStage(ctx, s3StageRead, started, err)
	return err
}

func readS3Part(
	ctx context.Context,
	source Source,
	meta storage.SnapshotMeta,
	manifest *S3Manifest,
	options S3ExportOptions,
	number uint32,
	buffer *s3PartBuffer,
	progress *s3ExportProgress,
) (s3PartUpload, error) {

	if err := ctx.Err(); err != nil {
		return s3PartUpload{}, err
	}

	start, end, err := partitionChunkRange(
		meta.ChunkCount,
		number,
		manifest.PartCount,
	)
	if err != nil {
		return s3PartUpload{}, err
	}

	length, err := partitionSize(meta.Size, start, end)
	if err != nil {
		return s3PartUpload{}, err
	}

	if length > options.PartSize {
		return s3PartUpload{}, fmt.Errorf(
			"part length %v exceeds %v",
			length,
			options.PartSize,
		)
	}

	started := time.Now()
	progress.startPart(uint64(number))
	defer progress.finishPart()
	if uint64(cap(buffer.data)) < length {
		buffer.data = make([]byte, int(length))
	} else {
		buffer.data = buffer.data[:int(length)]
	}

	err = readS3PartChunks(
		ctx,
		source,
		manifest.SnapshotID,
		meta,
		start,
		end,
		buffer.data,
		options.ReadWorkerCount,
		progress,
		options.MaxRetries,
	)
	progress.metrics.recordStage(ctx, s3StageAssemble, started, err)
	if err != nil {
		return s3PartUpload{}, err
	}

	return s3PartUpload{
		buffer: buffer,
		part: S3ManifestPart{
			Number: number,
			Key:    s3PartKey(number, options.Compression),
			Offset: uint64(start) * chunkSize,
			Length: length,
		},
		started: started,
		ready:   time.Now(),
	}, nil
}

// Workers decode and verify full chunks directly in disjoint part slices.
// All workers are joined before returning so the buffer can be uploaded or
// returned to the pool without racing a canceled read.
func readS3PartChunks(
	ctx context.Context,
	source Source,
	snapshotID string,
	meta storage.SnapshotMeta,
	start uint32,
	end uint32,
	data []byte,
	readWorkerCount int,
	progress *s3ExportProgress,
	maxRetries int,
) error {

	var entries []storage.ChunkMapEntry
	var hasEntry []bool
	err := retryS3Call(ctx, maxRetries, func() error {
		var err error
		entries, hasEntry, err = readStreamChunkMap(
			ctx, source, snapshotID, start, end, meta.ChunkCount,
		)
		return err
	})
	if err != nil {
		return err
	}

	workerCount := readWorkerCount
	if uint64(workerCount) > uint64(end-start) {
		workerCount = int(end - start)
	}

	jobs := make(chan storage.ChunkMapEntry)
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(
		func() error {
			defer close(jobs)
			for index := start; index < end; index++ {
				if err := groupCtx.Err(); err != nil {
					return err
				}

				localIndex := index - start
				if hasEntry[localIndex] && entries[localIndex].ChunkID != "" {
					select {
					case jobs <- entries[localIndex]:
					case <-groupCtx.Done():
						return groupCtx.Err()
					}

					continue
				}

				// A reused part can contain data from a previous upload. Clear
				// both explicit zero chunks and gaps in the chunk map.
				offset := int(localIndex) * chunkSize
				limit := offset + chunkSize
				if limit > len(data) {
					limit = len(data)
				}

				clear(data[offset:limit])
				progress.addRead(uint64(limit - offset))
			}

			return nil
		},
	)
	for i := 0; i < workerCount; i++ {
		group.Go(
			func() error {
				for entry := range jobs {
					if err := groupCtx.Err(); err != nil {
						return err
					}

					offset := int(entry.ChunkIndex-start) * chunkSize
					limit := offset + chunkSize
					if limit > len(data) {
						limit = len(data)
					}

					chunkData := data[offset:limit:limit]
					partial := len(chunkData) != chunkSize
					if partial {
						// The checksum covers the whole stored chunk, including
						// bytes beyond the logical end of the snapshot.
						chunkData = make([]byte, chunkSize)
					}

					chunk := common.Chunk{
						ID:         entry.ChunkID,
						Index:      entry.ChunkIndex,
						StoredInS3: entry.StoredInS3,
						Data:       chunkData,
					}

					if err := source.ReadChunk(groupCtx, &chunk); err != nil {
						return err
					}

					if partial {
						copy(data[offset:limit], chunkData[:limit-offset])
					}

					progress.addRead(uint64(limit - offset))
				}

				return nil
			},
		)
	}

	if err := group.Wait(); err != nil {
		return err
	}

	return ctx.Err()
}

func exportS3Parts(
	ctx context.Context,
	source Source,
	destination S3Destination,
	meta storage.SnapshotMeta,
	options S3ExportOptions,
	prefix string,
	manifest *S3Manifest,
	progress *s3ExportProgress,
	started time.Time,
	saveState func(context.Context) error,
) error {

	// The final upload may have been checkpointed before the old runner sorted
	// the parts. A fully uploaded checkpoint still needs stream order.
	sort.Slice(manifest.Parts, func(i, j int) bool {
		return manifest.Parts[i].Number < manifest.Parts[j].Number
	})

	if uint64(len(manifest.Parts)) == uint64(manifest.PartCount) {
		return ctx.Err()
	}

	payloadSize := options.PartSize
	if meta.Size < payloadSize {
		payloadSize = meta.Size
	}

	uri := "s3://" + options.Bucket + "/" + prefix
	workerCount := options.UploadWorkerCount
	if uint64(workerCount) > uint64(manifest.PartCount) {
		workerCount = int(manifest.PartCount)
	}

	// Assemblers and upload workers share this bounded pool. A buffer is
	// returned only after its readers or synchronous upload and retries end.
	buffers := make(chan *s3PartBuffer, workerCount+1)
	for i := 0; i < cap(buffers); i++ {
		buffers <- &s3PartBuffer{data: make([]byte, 0, int(payloadSize))}
	}

	jobs := make(chan s3PartUpload)
	completed := make(chan s3PartUpload)
	copyCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	group, groupCtx := errgroup.WithContext(copyCtx)
	// Immutable while workers run; the collector appends to manifest.Parts.
	uploadedParts := make(map[uint32]bool, len(manifest.Parts))
	for _, part := range manifest.Parts {
		uploadedParts[part.Number] = true
	}
	source = newS3ReadSource(
		source,
		options.ReadWorkerCount,
		progress,
		options.MaxRetries,
	)
	group.Go(
		func() error {
			defer close(jobs)
			var readers sync.WaitGroup
			// Keep the producer alive while it adds and joins assemblers.
			// No assembler can send a ready part after jobs is closed.
			defer readers.Wait()
			for number := uint64(1); number <= uint64(manifest.PartCount); number++ {
				if uploadedParts[uint32(number)] {
					continue
				}
				progress.setStage("waiting for buffer")
				var buffer *s3PartBuffer
				waitStarted := time.Now()
				progress.metrics.addActive(s3StageBufferWait, 1)
				select {
				case buffer = <-buffers:
				case <-groupCtx.Done():
					progress.metrics.addActive(s3StageBufferWait, -1)
					progress.metrics.recordStage(
						groupCtx, s3StageBufferWait, waitStarted, groupCtx.Err(),
					)
					return fmt.Errorf(
						"backup %v stopped before part %v/%v: %w",
						uri,
						number,
						manifest.PartCount,
						groupCtx.Err(),
					)
				}

				progress.metrics.addActive(s3StageBufferWait, -1)
				progress.metrics.recordStage(
					groupCtx, s3StageBufferWait, waitStarted, nil,
				)

				partNumber := uint32(number)
				readers.Add(1)
				group.Go(
					func() error {
						defer readers.Done()
						part, err := readS3Part(
							groupCtx,
							source,
							meta,
							manifest,
							options,
							partNumber,
							buffer,
							progress,
						)
						if err != nil {
							buffers <- buffer
							return fmt.Errorf(
								"backup %v reading part %v/%v: %w",
								uri,
								partNumber,
								manifest.PartCount,
								err,
							)
						}

						progress.addReadyPart()
						select {
						case jobs <- part:
							return nil
						case <-groupCtx.Done():
							progress.removeReadyPart()
							progress.metrics.recordStage(
								groupCtx, s3StageUploadWait, part.ready, groupCtx.Err(),
							)
							buffers <- buffer
							return fmt.Errorf(
								"backup %v stopped before uploading part %v/%v: %w",
								uri,
								partNumber,
								manifest.PartCount,
								groupCtx.Err(),
							)
						}
					},
				)
			}

			progress.setStage("waiting for uploads")
			return nil
		},
	)

	for i := 0; i < workerCount; i++ {
		group.Go(
			func() error {
				var encoder *s3PartEncoder
				for {
					var part s3PartUpload
					select {
					case <-groupCtx.Done():
						return fmt.Errorf(
							"backup %v upload worker stopped: %w",
							uri,
							groupCtx.Err(),
						)
					case job, ok := <-jobs:
						if !ok {
							return nil
						}

						part = job
					}

					progress.removeReadyPart()
					progress.metrics.recordStage(
						groupCtx, s3StageUploadWait, part.ready, nil,
					)
					if err := groupCtx.Err(); err != nil {
						buffers <- part.buffer
						return fmt.Errorf(
							"backup %v upload worker stopped: %w",
							uri,
							err,
						)
					}

					payload := part.buffer.data
					if options.Compression == "lz4" {
						started := time.Now()
						progress.startCompression()
						var err error
						if encoder == nil {
							encoder, err = newS3PartEncoder(payloadSize)
						}

						if err == nil {
							payload, err = encoder.encode(groupCtx, payload)
						}

						progress.finishCompression()
						progress.metrics.recordStage(
							groupCtx, s3StageCompress, started, err,
						)
						if err != nil {
							buffers <- part.buffer
							return fmt.Errorf(
								"backup %v compressing part %v/%v: %w",
								uri,
								part.part.Number,
								manifest.PartCount,
								err,
							)
						}
					}

					part.part.StoredLength = uint64(len(payload))
					sum := sha256.Sum256(payload)
					part.part.SHA256 = hex.EncodeToString(sum[:])
					progress.startUpload()
					uploadStarted := time.Now()
					err := destination.PutObjectWithOptions(
						groupCtx,
						options.Bucket,
						prefix+part.part.Key,
						persistence.S3Object{Data: payload},
						persistence.S3PutObjectOptions{
							ContentType:   "application/octet-stream",
							ContentMD5:    true,
							ContentSHA256: part.part.SHA256,
						},
					)
					progress.finishUpload()
					progress.metrics.recordStage(
						groupCtx, s3StageUpload, uploadStarted, err,
					)
					part.finished = time.Now()
					buffers <- part.buffer
					part.buffer = nil
					if err != nil {
						return fmt.Errorf(
							"backup %v uploading part %v/%v: %w",
							uri,
							part.part.Number,
							manifest.PartCount,
							err,
						)
					}

					progress.addCompleted(
						part.part.Length,
						part.part.StoredLength,
					)
					// The collector drains until all workers have joined, including
					// successful PUTs that finish concurrently with cancellation.
					completed <- part
				}
			},
		)
	}

	done := make(chan error, 1)
	go func() {
		err := group.Wait()
		close(completed)
		done <- err
	}()

	var uploaded uint64
	var stored uint64
	for _, part := range manifest.Parts {
		uploaded += part.Length
		stored += part.StoredLength
	}

	initialStored := stored
	var saveError error
	for part := range completed {
		manifest.Parts = append(manifest.Parts, part.part)
		if saveError == nil {
			if err := saveState(ctx); err != nil {
				saveError = err
				cancel()
			}
		}
		uploaded += part.part.Length
		stored += part.part.StoredLength
		now := time.Now()
		logging.Info(
			ctx,
			"backup %v: completed %v/%v logical bytes, stored %v bytes, "+
				"completed parts %v/%v, last part %v, "+
				"stored speed %.2f MiB/s, average %.2f MiB/s, ratio %.3f",
			uri,
			uploaded,
			meta.Size,
			stored,
			len(manifest.Parts),
			manifest.PartCount,
			part.part.Number,
			copySpeedMiBPerSecond(
				part.part.StoredLength,
				part.finished.Sub(part.started),
			),
			copySpeedMiBPerSecond(stored-initialStored, now.Sub(started)),
			s3CompressionRatio(uploaded, stored),
		)
	}

	// Upload completion order is independent of the raw stream order.
	sort.Slice(
		manifest.Parts,
		func(i, j int) bool {
			return manifest.Parts[i].Number < manifest.Parts[j].Number
		},
	)
	err := <-done
	if saveError != nil {
		return saveError
	}

	return err
}
