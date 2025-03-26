package common

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	net_url "net/url"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common/cache"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type Reader interface {
	EnableCache()

	// Returns total amount of bytes that can be read.
	Size() uint64

	ETag() string

	// Returns the number of bytes read.
	Read(ctx context.Context, start uint64, data []byte) (uint64, error)

	ReadBinary(
		ctx context.Context,
		start uint64,
		size uint64,
		byteOrder binary.ByteOrder,
		data interface{},
	) error

	CacheMissedRequestsCount() uint64
}

////////////////////////////////////////////////////////////////////////////////

type urlReader struct {
	httpClient httpClientInterface
	url        string
	etag       string
	size       uint64
	cache      *cache.Cache
}

func NewURLReader(
	ctx context.Context,
	httpClientTimeout time.Duration,
	httpClientMinRetryTimeout time.Duration,
	httpClientMaxRetryTimeout time.Duration,
	httpClientMaxRetries uint32,
	url string,
) (Reader, error) {

	parsed, err := net_url.Parse(url)
	if err != nil {
		return nil, NewSourceInvalidError("parse url: %w", err)
	}

	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return nil, NewSourceInvalidError(
			"invalid protocol scheme %q",
			parsed.Scheme,
		)
	}

	httpClient := newHTTPClient(
		ctx,
		httpClientTimeout,
		httpClientMinRetryTimeout,
		httpClientMaxRetryTimeout,
		httpClientMaxRetries,
		url,
	)

	resp, err := httpClient.Head(ctx)
	if err != nil {
		return nil, err
	}

	etag := resp.Header.Get("Etag")
	if len(etag) == 0 {
		return nil, NewSourceInvalidError(
			"missing Etag header in response",
		)
	}

	if resp.ContentLength == 0 {
		return nil, NewSourceInvalidError(
			"url ContentLength should not be zero",
		)
	}

	return &urlReader{
		httpClient: httpClient,
		url:        url,
		etag:       etag,
		size:       uint64(resp.ContentLength),
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *urlReader) EnableCache() {
	if r.cache == nil {
		r.cache = cache.NewCache(r.read)
	}
}

func (r *urlReader) Size() uint64 {
	return r.size
}

func (r *urlReader) ETag() string {
	return r.etag
}

func (r *urlReader) CacheMissedRequestsCount() uint64 {
	return r.httpClient.RequestsCount()
}

func (r *urlReader) validateRange(
	start, end uint64, // Half-open interval [start:end).
) error {

	if start >= end || end > r.size {
		return NewSourceInvalidError("range [%v:%v) is invalid", start, end)
	}

	return nil
}

func (r *urlReader) read(
	ctx context.Context,
	start uint64,
	data []byte,
) error {

	end := start + uint64(len(data))
	if end > r.size {
		end = r.size

		if r.size <= start {
			return NewSourceInvalidError(
				"size %v should be greater than start %v",
				r.size,
				start,
			)
		}

		// Cut the tail.
		data = data[:r.size-start]
	}

	err := r.validateRange(start, end)
	if err != nil {
		return err
	}

	reader, err := r.httpClient.Body(ctx, start, end, r.etag)
	if err != nil {
		return err
	}
	defer reader.Close()

	_, err = io.ReadFull(reader, data)
	if err != nil {
		// NBS-3324: interpret all errors as retriable.
		return errors.NewRetriableError(err)
	}

	return nil
}

func (r *urlReader) Read(
	ctx context.Context,
	start uint64,
	data []byte,
) (uint64, error) {

	size := uint64(len(data))

	if r.cache == nil {
		err := r.read(ctx, start, data)
		if err != nil {
			return 0, err
		}

		return size, nil
	}

	err := r.cache.Read(ctx, start, data)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (r *urlReader) ReadBinary(
	ctx context.Context,
	start uint64,
	size uint64,
	byteOrder binary.ByteOrder,
	data interface{},
) error {

	end := start + size

	err := r.validateRange(start, end)
	if err != nil {
		return err
	}

	byteData := make([]byte, size)

	_, err = r.Read(ctx, start, byteData)
	if err != nil {
		return err
	}

	logging.Info(ctx, "bytedata is %v", byteData)
	err = binary.Read(bytes.NewReader(byteData), byteOrder, data)
	// NBS-3324: interpret all errors as retriable.
	if err != nil {
		return errors.NewRetriableError(err)
	}

	return nil
}
