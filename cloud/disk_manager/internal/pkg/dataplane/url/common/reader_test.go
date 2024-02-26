package common

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

func randomData(t *testing.T, size uint64) []byte {
	data := make([]byte, size)
	_, err := rand.Read(data)
	require.NoError(t, err)
	return data
}

////////////////////////////////////////////////////////////////////////////////

type bytesReadCloser struct {
	*bytes.Reader
}

func (r *bytesReadCloser) Close() error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type httpClientMock struct {
	data []byte
}

func (h httpClientMock) Head(ctx context.Context) (*http.Response, error) {
	return &http.Response{
		Status:        "OK",
		StatusCode:    200,
		Header:        map[string][]string{"Etag": {"Example"}},
		ContentLength: int64(len(h.data)),
	}, nil
}

func (h httpClientMock) Body(
	ctx context.Context,
	start, end uint64,
	etag string,
) (io.ReadCloser, error) {

	buffer := &bytesReadCloser{bytes.NewReader(h.data[start:end])}
	return buffer, nil
}

func (h httpClientMock) RequestsCount() uint64 {
	return 0
}

////////////////////////////////////////////////////////////////////////////////

func newTestReader(t *testing.T, data []byte) *urlReader {
	httpClient := &httpClientMock{data: data}

	resp, err := httpClient.Head(newContext())
	require.NoError(t, err)

	reader := &urlReader{
		httpClient: httpClient,
		url:        "",
		etag:       resp.Header.Get("Etag"),
		size:       uint64(resp.ContentLength),
	}
	reader.EnableCache()
	return reader
}

////////////////////////////////////////////////////////////////////////////////

func checkRead(
	t *testing.T,
	reader *urlReader,
	expectedData []byte,
	start uint64,
	size uint64,
) {

	data := make([]byte, size)
	bytesRead, err := reader.Read(newContext(), start, data)

	require.NoError(t, err)
	require.Equal(t, uint64(len(data)), bytesRead)
	expected := expectedData[start : start+size]
	require.Equal(t, expected, data)
}

////////////////////////////////////////////////////////////////////////////////

func TestReader(t *testing.T) {
	chunkSize := uint64(4 * 1024 * 1024)
	testCases := [][]struct {
		name  string
		start uint64
		size  uint64
	}{
		{
			{"readOnBorder", chunkSize - 2048, 4096},
			{"readFromLeft", 0, chunkSize * 3},
			{"readFromRight", chunkSize * 2, chunkSize * 3},
		},
		{
			{"readOne", 0, 1},
			{"readCached", 0, chunkSize},
		},
	}

	for _, testCaseSeries := range testCases {
		for _, testCase := range testCaseSeries {
			expectedData := randomData(t, 5*chunkSize)
			reader := newTestReader(t, expectedData)

			t.Run(testCase.name, func(t *testing.T) {
				checkRead(
					t,
					reader,
					expectedData,
					testCase.start,
					testCase.size,
				)
			})
		}
	}
}
