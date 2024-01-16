package testing

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/url/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type formatReaderFunc func(
	t *testing.T,
	ctx context.Context,
	reader common.Reader,
) common.ImageMapReader

////////////////////////////////////////////////////////////////////////////////

func hasNonZeroContent(i common.ImageMapItem) bool {
	return i.Data && !i.Zero && i.RawOffset != nil
}

func sameContentTypeTo(a, b common.ImageMapItem) bool {
	return a.Data == b.Data && a.Zero == b.Zero
}

func isPrevTo(a, b common.ImageMapItem) bool {
	return b.Start == a.Start+a.Length
}

func rawIsPrevTo(a, b common.ImageMapItem) bool {
	return hasNonZeroContent(a) == hasNonZeroContent(b) &&
		(!hasNonZeroContent(a) || *b.RawOffset == *a.RawOffset+a.Length)
}

////////////////////////////////////////////////////////////////////////////////

func loadImageMapItems(t *testing.T, filepath string) []common.ImageMapItem {
	var items []common.ImageMapItem

	file, err := os.Open(filepath)
	require.NoError(t, err)

	data, err := ioutil.ReadAll(file)
	require.NoError(t, err)

	err = json.Unmarshal(data, &items)
	require.NoError(t, err)
	return items
}

func mergeCompressedImageMapItems(
	items []common.ImageMapItem,
) []common.ImageMapItem {

	var res []common.ImageMapItem

	currentItem := items[0]

	for _, item := range items[1:] {
		if sameContentTypeTo(currentItem, item) &&
			isPrevTo(currentItem, item) &&
			rawIsPrevTo(currentItem, item) {

			currentItem.Length += item.Length
		} else {
			currentItem.CompressedOffset = nil
			currentItem.CompressedSize = nil
			currentItem.CompressionType = 0
			res = append(res, currentItem)
			currentItem = item
		}
	}

	currentItem.CompressedOffset = nil
	currentItem.CompressedSize = nil
	currentItem.CompressionType = 0
	res = append(res, currentItem)
	return res
}

func readImageMapItems(
	t *testing.T,
	ctx context.Context,
	reader common.ImageMapReader,
) []common.ImageMapItem {

	var items []common.ImageMapItem
	var err error

	for {
		items, err = reader.Read(ctx)
		if !errors.CanRetry(err) {
			break
		}
	}

	require.NoError(t, err)
	return items
}

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func MapImageTest(
	t *testing.T,
	url string,
	expectedMapFile string,
	formatReader formatReaderFunc,
) {

	ctx := newContext()

	reader, err := common.NewURLReader(
		ctx,
		time.Minute,   // httpClientTimeout
		time.Second,   // httpClientMinRetryTimeout
		8*time.Second, // httpClientMaxRetryTimeout
		5,             // httpClientMaxRetries
		url,
	)
	require.NoError(t, err)

	expectedItems := loadImageMapItems(t, expectedMapFile)

	imageFormatReader := formatReader(t, ctx, reader)
	items := readImageMapItems(t, ctx, imageFormatReader)
	items = mergeCompressedImageMapItems(items)

	require.Equal(t, expectedItems, items)
}
