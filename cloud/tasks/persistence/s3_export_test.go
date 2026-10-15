package persistence

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/stretchr/testify/require"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
)

func newExportTestS3Client(
	t *testing.T,
	endpoint string,
	region string,
	retries uint64,
	timeout time.Duration,
) *S3Client {

	t.Helper()
	client, err := NewS3Client(
		endpoint,
		region,
		S3Credentials{ID: "id", Secret: "secret", SessionToken: "session-token"},
		timeout,
		empty.NewRegistry(),
		retries,
		nil,
		nil,
		0, // maxIdleConnsPerHost
		WithS3EmptySigningRegion(),
	)
	require.NoError(t, err)
	return client
}

func TestS3ExportCredentialsSessionToken(t *testing.T) {
	for _, token := range []string{"", "temporary-token"} {
		t.Run(
			token,
			func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "credentials.json")
				require.NoError(
					t,
					os.WriteFile(
						file,
						[]byte(`{"id":"id","secret":"secret","session_token":"`+token+`"}`),
						0600,
					),
				)
				credentials, err := NewS3CredentialsFromFile(file)
				require.NoError(t, err)
				require.Equal(
					t,
					S3Credentials{ID: "id", Secret: "secret", SessionToken: token},
					credentials,
				)
			},
		)
	}
}

func TestS3ExportUploadHeadersAndStableRetries(t *testing.T) {
	for _, region := range []string{"", "custom-region"} {
		t.Run(
			region,
			func(t *testing.T) {
				type received struct {
					path          string
					contentType   string
					encoding      string
					md5           string
					sha256        string
					authorization string
					token         string
					body          string
				}

				requests := make(chan received, 4)
				var attempts atomic.Int32
				server := httptest.NewServer(
					http.HandlerFunc(
						func(w http.ResponseWriter, r *http.Request) {
							data, _ := io.ReadAll(r.Body)
							requests <- received{
								r.URL.Path,
								r.Header.Get("Content-Type"),
								r.Header.Get("Content-Encoding"),
								r.Header.Get("Content-MD5"),
								r.Header.Get("X-Amz-Content-Sha256"),
								r.Header.Get("Authorization"),
								r.Header.Get("X-Amz-Security-Token"),
								string(data),
							}

							if attempts.Add(1) <= 2 {
								w.WriteHeader(http.StatusServiceUnavailable)
								_, _ = io.WriteString(
									w,
									`<Error><Code>SlowDown</Code><Message>retry</Message></Error>`,
								)
							}
						},
					),
				)
				defer server.Close()
				client := newExportTestS3Client(t, server.URL, region, 2, 5*time.Second)
				data := []byte("exact buffered raw bytes")
				payloadSum := sha256.Sum256(data)
				payloadSHA256 := hex.EncodeToString(payloadSum[:])
				err := client.PutObjectWithOptions(
					newContext(),
					"bucket",
					"backup/part-00000001.raw",
					S3Object{Data: data},
					S3PutObjectOptions{
						ContentType:   "application/octet-stream",
						ContentMD5:    true,
						ContentSHA256: payloadSHA256,
					},
				)
				require.NoError(t, err)
				require.Equal(t, int32(3), attempts.Load())
				sum := md5.Sum(data)
				for i := 0; i < 3; i++ {
					r := <-requests
					require.Equal(t, "/bucket/backup/part-00000001.raw", r.path)
					require.Equal(t, string(data), r.body)
					require.Equal(t, base64.StdEncoding.EncodeToString(sum[:]), r.md5)
					require.Equal(t, payloadSHA256, r.sha256)
					require.Equal(t, "application/octet-stream", r.contentType)
					require.Empty(t, r.encoding)
					require.Equal(t, "session-token", r.token)
					require.Contains(t, r.authorization, "/"+region+"/s3/aws4_request")
				}
			},
		)
	}
}

func TestS3ExportInvalidPrecomputedHashDoesNotSendRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
			},
		),
	)
	defer server.Close()
	client := newExportTestS3Client(t, server.URL, "region", 2, time.Second)
	for _, hash := range []string{
		"bad", strings.Repeat("z", 64), strings.Repeat("a", 62),
	} {
		err := client.PutObjectWithOptions(
			newContext(), "bucket", "key", S3Object{Data: []byte("data")},
			S3PutObjectOptions{ContentSHA256: hash},
		)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	}

	require.Zero(t, calls.Load())
}

type exportTestCountingBody struct {
	io.ReadSeeker
	readBytes *atomic.Int64
}

func (b *exportTestCountingBody) Read(data []byte) (int, error) {
	n, err := b.ReadSeeker.Read(data)
	b.readBytes.Add(int64(n))
	return n, err
}

func TestS3ExportPrecomputedHashSkipsSDKBodyScan(t *testing.T) {
	for _, precomputed := range []bool{false, true} {
		name := "sdk-hash"
		if precomputed {
			name = "precomputed-hash"
		}

		t.Run(
			name,
			func(t *testing.T) {
				data := []byte("raw snapshot part")
				received := make(chan []byte, 1)
				server := httptest.NewServer(
					http.HandlerFunc(
						func(w http.ResponseWriter, r *http.Request) {
							body, _ := io.ReadAll(r.Body)
							received <- body
						},
					),
				)
				defer server.Close()
				client := newExportTestS3Client(t, server.URL, "region", 0, time.Second)
				var readBytes atomic.Int64
				// Client build handlers run before the S3 per-request body-hash
				// handler. Count reads during build/sign separately from transport.
				client.s3.Handlers.Build.PushBack(
					func(r *request.Request) {
						r.SetReaderBody(&exportTestCountingBody{r.Body, &readBytes})
					},
				)
				beforeSend := make(chan int64, 1)
				client.s3.Handlers.Send.PushFront(
					func(r *request.Request) {
						beforeSend <- readBytes.Load()
					},
				)
				options := S3PutObjectOptions{ContentMD5: true}
				if precomputed {
					sum := sha256.Sum256(data)
					options.ContentSHA256 = hex.EncodeToString(sum[:])
				}

				err := client.PutObjectWithOptions(
					newContext(), "bucket", "part.raw", S3Object{Data: data}, options,
				)
				require.NoError(t, err)
				require.Equal(t, data, <-received)
				if precomputed {
					require.Zero(t, <-beforeSend)
				} else {
					require.EqualValues(t, len(data), <-beforeSend)
				}
			},
		)
	}
}

func TestS3ExportPermanentErrorsAndRetryLimit(t *testing.T) {
	for _, tc := range []struct {
		code      string
		status    int
		calls     int
		permanent bool
	}{
		{"AccessDenied", 403, 1, true},
		{"ExpiredToken", 400, 1, true},
		{"InvalidAccessKeyId", 403, 1, true},
		{"SignatureDoesNotMatch", 403, 1, true},
		{"NoSuchBucket", 404, 1, true},
		{"EntityTooLarge", 400, 1, true},
		{"InvalidRequest", 400, 1, true},
		{"BadDigest", 400, 1, true},
		{"InternalError", 500, 3, false},
		{"SlowDown", 503, 3, false},
		{"RequestTimeout", 400, 3, false},
		{"Throttling", 400, 3, false},
	} {
		t.Run(
			tc.code,
			func(t *testing.T) {
				var attempts atomic.Int32
				server := httptest.NewServer(
					http.HandlerFunc(
						func(w http.ResponseWriter, r *http.Request) {
							attempts.Add(1)
							w.WriteHeader(tc.status)
							_, _ = io.WriteString(
								w,
								"<Error><Code>"+tc.code+"</Code><Message>failed</Message></Error>",
							)
						},
					),
				)
				defer server.Close()
				client := newExportTestS3Client(t, server.URL, "region", 2, 5*time.Second)
				err := client.PutObjectWithOptions(
					newContext(),
					"bucket",
					"key",
					S3Object{Data: []byte("data")},
					S3PutObjectOptions{},
				)
				require.Error(t, err)
				require.Equal(t, int32(tc.calls), attempts.Load())
				require.Equal(
					t,
					tc.permanent,
					errors.Is(err, errors.NewEmptyNonRetriableError()),
				)
			},
		)
	}
}

func TestS3ExportReadBackLimitAndBucketCheck(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	server := httptest.NewServer(
		http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(int(status.Load()))
				if r.Method == http.MethodGet {
					_, _ = io.WriteString(w, `{"manifest":true}`)
				}
			},
		),
	)
	defer server.Close()
	client := newExportTestS3Client(t, server.URL, "", 0, time.Second)
	exists, err := client.BucketExists(newContext(), "bucket")
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = client.ObjectExists(newContext(), "bucket", "manifest.json")
	require.NoError(t, err)
	require.True(t, exists)
	data, err := client.GetObjectWithLimit(
		newContext(),
		"bucket",
		"manifest.json",
		17,
	)
	require.NoError(t, err)
	require.Equal(t, `{"manifest":true}`, string(data))
	_, err = client.GetObjectWithLimit(newContext(), "bucket", "manifest.json", 4)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds read limit")
	status.Store(http.StatusNotFound)
	exists, err = client.BucketExists(newContext(), "bucket")
	require.NoError(t, err)
	require.False(t, exists)
	exists, err = client.ObjectExists(newContext(), "bucket", "manifest.json")
	require.NoError(t, err)
	require.False(t, exists)
	status.Store(http.StatusForbidden)
	_, err = client.BucketExists(newContext(), "bucket")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	_, err = client.ObjectExists(newContext(), "bucket", "manifest.json")
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestS3ExportTimeoutAndCancellation(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "cancel", true: "timeout"}[timeout],
			func(t *testing.T) {
				started := make(chan struct{}, 1)
				server := httptest.NewServer(
					http.HandlerFunc(
						func(w http.ResponseWriter, r *http.Request) {
							_, _ = io.Copy(io.Discard, r.Body)
							started <- struct{}{}
							<-r.Context().Done()
						},
					),
				)
				defer server.Close()
				requestTimeout := 5 * time.Second
				if timeout {
					requestTimeout = 100 * time.Millisecond
				}

				client := newExportTestS3Client(t, server.URL, "region", 3, requestTimeout)
				ctx, cancel := context.WithCancel(newContext())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					done <- client.PutObjectWithOptions(
						ctx,
						"bucket",
						"key",
						S3Object{Data: []byte("data")},
						S3PutObjectOptions{},
					)
				}()

				select {
				case <-started:
				case <-time.After(5 * time.Second):
					t.Fatal("request did not start")
				}

				if !timeout {
					cancel()
				}

				select {
				case err := <-done:
					require.Error(t, err)
					require.True(
						t,
						strings.Contains(
							err.Error(),
							"canceled",
						) || strings.Contains(
							err.Error(),
							"deadline",
						),
					)
				case <-time.After(5 * time.Second):
					t.Fatal("request did not stop")
				}
			},
		)
	}
}
