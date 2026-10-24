package backup

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// The fixture speaks HTTP to the real S3 client. It never calls the backup
// implementation to produce an expected object.
type backupHTTPObject struct {
	data    []byte
	headers http.Header
}

type backupHTTPStore struct {
	mu         sync.Mutex
	objects    map[string]backupHTTPObject
	failMethod string
	requests   int
}

func (s *backupHTTPStore) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests++
	if r.Method == s.failMethod {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "<Error><Code>ServiceUnavailable</Code><Message>injected</Message></Error>")
		return
	}
	switch r.Method {
	case http.MethodPut:
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.objects[r.URL.Path] = backupHTTPObject{data: data, headers: r.Header.Clone()}
		w.Header().Set("ETag", "\"fixture\"")
	case http.MethodGet:
		object, ok := s.objects[r.URL.Path]
		if !ok {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "<Error><Code>NoSuchKey</Code></Error>")
			return
		}
		for name, values := range object.headers {
			if strings.HasPrefix(strings.ToLower(name), "x-amz-meta-") {
				w.Header()[name] = append([]string(nil), values...)
			}
		}
		_, _ = w.Write(object.data)
	default:
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
	}
}

func newBackupHTTPFixture(t *testing.T, encrypted bool) (*S3, *backupHTTPStore) {
	t.Helper()
	store := &backupHTTPStore{objects: make(map[string]backupHTTPObject)}
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	client, err := persistence.NewS3Client(
		server.URL, "test", persistence.NewS3Credentials("test", "test"),
		5*time.Second, metrics.NewEmptyRegistry(), 0, nil, nil,
	)
	require.NoError(t, err)
	var key []byte
	keyID := ""
	if encrypted {
		key = make([]byte, keySize)
		keyID = "fixture-key"
	}
	backupS3, err := NewS3(client, "backup", "isolated", keyID, key)
	require.NoError(t, err)
	return backupS3, store
}

func TestBackupS3HTTPRoundTripAndRetry(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		name := "plain"
		if encrypted {
			name = "encrypted"
		}
		t.Run(name, func(t *testing.T) {
			s3, store := newBackupHTTPFixture(t, encrypted)
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
			dek, err := s3.EnsureEncryptedDEK(nil)
			require.NoError(t, err)
			checksum := "independent-checksum"
			input := persistence.S3Object{
				Data:         []byte{0, 1, 2, 0, 255},
				Metadata:     map[string]*string{"Checksum": &checksum},
				StorageClass: "STANDARD_IA",
			}
			key := ChunkKey("snapshot.chunk")
			require.NoError(t, s3.PutObject(ctx, key, dek, input))
			require.Len(t, input.Metadata, 1, "PutObject must not mutate caller metadata")
			store.mu.Lock()
			raw := store.objects["/backup/isolated/chunks/snapshot.chunk"]
			store.mu.Unlock()
			require.Equal(t, "STANDARD_IA", raw.headers.Get("X-Amz-Storage-Class"))
			require.Equal(t, checksum, raw.headers.Get("X-Amz-Meta-Checksum"))
			if encrypted {
				require.NotEqual(t, input.Data, raw.data)
				require.Equal(t, "fixture-key", raw.headers.Get("X-Amz-Meta-Key-Id"))
				require.NotEmpty(t, raw.headers.Get("X-Amz-Meta-Encrypted-Dek"))
			} else {
				require.Equal(t, input.Data, raw.data)
				require.Empty(t, raw.headers.Get("X-Amz-Meta-Key-Id"))
			}
			got, err := s3.GetObject(ctx, key)
			require.NoError(t, err)
			require.Equal(t, input.Data, got.Data)
			require.Equal(t, input.Metadata, got.Metadata)

			// A failed replacement must leave the previous complete object usable.
			store.mu.Lock()
			store.failMethod = http.MethodPut
			store.mu.Unlock()
			replacement := persistence.S3Object{Data: []byte("replacement")}
			require.Error(t, s3.PutObject(ctx, key, dek, replacement))
			got, err = s3.GetObject(ctx, key)
			require.NoError(t, err)
			require.Equal(t, input.Data, got.Data)
			store.mu.Lock()
			store.failMethod = ""
			store.mu.Unlock()
			require.NoError(t, s3.PutObject(ctx, key, dek, replacement))
			got, err = s3.GetObject(ctx, key)
			require.NoError(t, err)
			require.Equal(t, replacement.Data, got.Data)

			store.mu.Lock()
			store.failMethod = http.MethodGet
			store.mu.Unlock()
			_, err = s3.GetObject(ctx, key)
			require.Error(t, err)
			store.mu.Lock()
			store.failMethod = ""
			store.mu.Unlock()
			got, err = s3.GetObject(ctx, key)
			require.NoError(t, err)
			require.Equal(t, replacement.Data, got.Data)
			_, err = s3.GetObject(ctx, "missing")
			require.Error(t, err)
		})
	}
}

func TestBackupS3HTTPRejectsIncompleteOrCorruptedObjects(t *testing.T) {
	cases := map[string]func(*backupHTTPObject){
		"missing-key-id": func(o *backupHTTPObject) { o.headers.Del("X-Amz-Meta-Key-Id") },
		"wrong-key-id":   func(o *backupHTTPObject) { o.headers.Set("X-Amz-Meta-Key-Id", "other") },
		"missing-dek":    func(o *backupHTTPObject) { o.headers.Del("X-Amz-Meta-Encrypted-Dek") },
		"invalid-base64": func(o *backupHTTPObject) { o.headers.Set("X-Amz-Meta-Encrypted-Dek", "!invalid!") },
		"truncated-dek":  func(o *backupHTTPObject) { o.headers.Set("X-Amz-Meta-Encrypted-Dek", "YWJj") },
		"truncated-data": func(o *backupHTTPObject) { o.data = o.data[:1] },
		"corrupt-data":   func(o *backupHTTPObject) { o.data[len(o.data)-1] ^= 1 },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s3, store := newBackupHTTPFixture(t, true)
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
			dek, err := s3.NewEncryptedDEK()
			require.NoError(t, err)
			require.NoError(t, s3.PutObject(ctx, "chunk", dek, persistence.S3Object{Data: []byte("expected")}))
			store.mu.Lock()
			object := store.objects["/backup/isolated/chunk"]
			mutate(&object)
			store.objects["/backup/isolated/chunk"] = object
			store.mu.Unlock()
			_, err = s3.GetObject(ctx, "chunk")
			require.Error(t, err, "incomplete/corrupt objects must never be returned as valid data")
		})
	}
}

func TestBackupS3HTTPBindsCiphertextToObjectKey(t *testing.T) {
	s3, store := newBackupHTTPFixture(t, true)
	ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
	dek, err := s3.NewEncryptedDEK()
	require.NoError(t, err)
	require.NoError(t, s3.PutObject(ctx, "original", dek, persistence.S3Object{Data: []byte("expected")}))
	store.mu.Lock()
	store.objects["/backup/isolated/substituted"] = store.objects["/backup/isolated/original"]
	store.mu.Unlock()
	_, err = s3.GetObject(ctx, "substituted")
	require.Error(t, err)
	// A failed read must not damage the original.
	got, err := s3.GetObject(ctx, "original")
	require.NoError(t, err)
	require.Equal(t, []byte("expected"), got.Data)
}

func TestBackupS3HTTPPlainReaderRejectsEncryptedObject(t *testing.T) {
	s3, _ := newBackupHTTPFixture(t, true)
	ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
	dek, err := s3.NewEncryptedDEK()
	require.NoError(t, err)
	require.NoError(t, s3.PutObject(ctx, "chunk", dek, persistence.S3Object{Data: []byte("expected")}))
	plain, err := NewS3(s3.s3, "backup", "isolated", "", nil)
	require.NoError(t, err)
	_, err = plain.GetObject(ctx, "chunk")
	require.Error(t, err)
}
