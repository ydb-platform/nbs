package url

import (
	"context"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	url_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/qcow2"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/vhd"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/vmdk"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultHTTPClientTimeout         = time.Minute
	defaultHTTPClientMinRetryTimeout = time.Second
	defaultHTTPClientMaxRetryTimeout = 8 * time.Second
	defaultHTTPClientMaxRetries      = 5
)

////////////////////////////////////////////////////////////////////////////////

type URLSource interface {
	dataplane_common.Source

	ETag() string

	CacheMissedRequestsCount() uint64
}

////////////////////////////////////////////////////////////////////////////////

type urlSource struct {
	chunkMap       map[uint32]chunkMapEntry
	chunkMapMutex  sync.Mutex
	chunkSize      uint64
	chunkIndices   common.ChannelWithInflightQueue
	reader         url_common.Reader
	chunkMapReader *chunkMapReader
}

func (s *urlSource) ChunkIndices(
	ctx context.Context,
	milestone dataplane_common.Milestone,
	processedChunkIndices <-chan uint32,
	holeChunkIndices common.ChannelWithCancellation,
) (<-chan uint32, common.ChannelWithCancellation, <-chan error) {

	common.Assert(s.chunkIndices.Empty(), "should be called once")

	inflightLimit := cap(processedChunkIndices)

	s.chunkIndices = common.NewChannelWithInflightQueue(
		common.Milestone{
			Value:               milestone.ChunkIndex,
			ProcessedValueCount: milestone.TransferredChunkCount,
		},
		processedChunkIndices,
		holeChunkIndices,
		inflightLimit,
	)

	errors := make(chan error, 1)

	go func() {
		defer close(errors)

		defer func() {
			if r := recover(); r != nil {
				errors <- task_errors.NewPanicError(r)
			}
		}()

		defer s.chunkIndices.Close()

		entries, err := s.chunkMapReader.read(ctx)
		if err != nil {
			errors <- err
			return
		}

		for _, entry := range entries {
			if entry.chunkIndex < milestone.ChunkIndex {
				continue
			}

			// Cache chunk ids for further reads.
			s.chunkMapMutex.Lock()
			s.chunkMap[entry.chunkIndex] = entry
			s.chunkMapMutex.Unlock()

			_, err := s.chunkIndices.Send(ctx, entry.chunkIndex)
			if err != nil {
				errors <- err
				return
			}
		}
	}()

	return s.chunkIndices.Channel(), common.ChannelWithCancellation{}, errors
}

func (s *urlSource) Read(
	ctx context.Context,
	chunk *dataplane_common.Chunk,
) error {

	s.chunkMapMutex.Lock()
	entry, ok := s.chunkMap[chunk.Index]
	s.chunkMapMutex.Unlock()

	if !ok {
		// TODO: maybe reread chunk map
		return task_errors.NewNonRetriableErrorf(
			"failed to find chunk id for index %d",
			chunk.Index,
		)
	}

	reader := newImageReader(entry.imageMap.Items, s.chunkSize, s.reader)
	err := reader.Read(ctx, chunk)
	if err != nil {
		return err
	}

	// Evict successully read chunk id to keep low memory footprint.
	s.chunkMapMutex.Lock()
	delete(s.chunkMap, chunk.Index)
	s.chunkMapMutex.Unlock()

	return nil
}

func (s *urlSource) Milestone() dataplane_common.Milestone {
	common.Assert(!s.chunkIndices.Empty(), "should not be nil")

	milestone := s.chunkIndices.Milestone()
	return dataplane_common.Milestone{
		ChunkIndex:            milestone.Value,
		TransferredChunkCount: milestone.ProcessedValueCount,
	}
}

func (s *urlSource) ChunkCount(ctx context.Context) (uint32, error) {
	return s.chunkMapReader.chunkCount(), nil
}

func (s *urlSource) Close(ctx context.Context) {
}

func (s *urlSource) ETag() string {
	return s.reader.ETag()
}

func (s *urlSource) CacheMissedRequestsCount() uint64 {
	return s.reader.CacheMissedRequestsCount()
}

////////////////////////////////////////////////////////////////////////////////

func NewURLSource(
	ctx context.Context,
	httpClientTimeout time.Duration,
	httpClientMinRetryTimeout time.Duration,
	httpClientMaxRetryTimeout time.Duration,
	httpClientMaxRetries uint32,
	url string,
	chunkSize uint64,
) (URLSource, error) {

	urlReader, err := url_common.NewURLReader(
		ctx,
		httpClientTimeout,
		httpClientMinRetryTimeout,
		httpClientMaxRetryTimeout,
		httpClientMaxRetries,
		url,
	)
	if err != nil {
		return nil, err
	}

	imageMapReader, err := newImageMapReader(ctx, urlReader)
	if err != nil {
		return nil, err
	}

	chunkMapReader, err := newChunkMapReader(ctx, imageMapReader, chunkSize)
	if err != nil {
		return nil, err
	}

	return &urlSource{
		chunkMap:       make(map[uint32]chunkMapEntry),
		chunkSize:      chunkSize,
		reader:         urlReader,
		chunkMapReader: chunkMapReader,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func GetImageFormat(ctx context.Context, url string) (ImageFormat, error) {
	urlReader, err := url_common.NewURLReader(
		ctx,
		defaultHTTPClientTimeout,
		defaultHTTPClientMinRetryTimeout,
		defaultHTTPClientMaxRetryTimeout,
		defaultHTTPClientMaxRetries,
		url,
	)
	if err != nil {
		return "", err
	}

	return guessImageFormat(ctx, urlReader)
}

////////////////////////////////////////////////////////////////////////////////

func newImageMapReader(
	ctx context.Context,
	urlReader url_common.Reader,
) (url_common.ImageMapReader, error) {

	format, err := guessImageFormat(ctx, urlReader)
	if err != nil {
		return nil, err
	}

	switch format {
	case ImageFormatQCOW2:
		reader, err := qcow2.NewImageMapReader(ctx, urlReader)
		if err != nil {
			return nil, err
		}

		urlReader.EnableCache()
		return reader, nil

	case ImageFormatVMDK:
		reader, err := vmdk.NewImageMapReader(ctx, urlReader)
		if err != nil {
			return nil, err
		}

		urlReader.EnableCache()
		return reader, nil

	case ImageFormatVHD:
		reader, err := vhd.NewImageMapReader(ctx, urlReader)
		if err != nil {
			return nil, err
		}

		urlReader.EnableCache()
		return reader, nil

	case ImageFormatRaw:
		return newRawImageMapReader(urlReader.Size()), nil

	default:
		return nil, url_common.NewSourceInvalidError(
			"unsupported image format: %s",
			format,
		)
	}
}
