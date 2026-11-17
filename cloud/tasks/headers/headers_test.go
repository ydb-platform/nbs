package headers

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestSetAuthorizationHeader(t *testing.T) {
	tests := []struct {
		name     string
		token    string
		expected string
	}{
		{
			name:     "raw token",
			token:    "test-token",
			expected: "Bearer test-token",
		},
		{
			name:     "prefixed token",
			token:    "Bearer test-token",
			expected: "Bearer test-token",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header := http.Header{}
			SetAuthorizationHeader(header, test.token)
			require.Equal(t, test.expected, header.Get("Authorization"))
		})
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestHeaders(t *testing.T) {
	ctx := context.Background()

	keyA := "A"
	keyB := "B"
	keyC := "C"
	keyD := "D"

	checkContext := func(ctx context.Context, expected map[string]string) {
		var keys []string
		for key := range expected {
			keys = append(keys, key)
		}
		keys = append(keys, "non_existing_key")

		actualIncoming := GetFromIncomingContext(ctx, keys)
		require.Equal(t, expected, actualIncoming)
		actualOutgoing := GetFromOutgoingContext(ctx, keys)
		require.Equal(t, expected, actualOutgoing)
	}

	checkContext(ctx, map[string]string{})

	ctx = Append(ctx, map[string]string{keyA: "a", keyB: "b"})
	checkContext(ctx, map[string]string{keyA: "a", keyB: "b"})
	ctx = Append(ctx, map[string]string{keyB: "bb", keyC: "c"})
	checkContext(ctx, map[string]string{keyA: "a", keyB: "b", keyC: "c"})

	ctx = Replace(ctx, map[string]string{keyB: "bbb"})
	checkContext(ctx, map[string]string{keyA: "a", keyB: "bbb", keyC: "c"})
	ctx = Replace(ctx, map[string]string{keyD: "d"})
	checkContext(ctx, map[string]string{keyA: "a", keyB: "bbb", keyC: "c"})
}

func TestTaskIDSurvivesSchedulerMetadata(t *testing.T) {
	ctx := SetIncomingOperationID(context.Background(), "compute-operation")
	require.Empty(t, GetTaskID(ctx))
	ctx = SetIncomingTaskID(ctx, "compute-task")
	metadata := GetTracingHeaders(ctx)
	require.Equal(t, "compute-task", metadata["x-task-id"])
	require.Equal(t, "compute-operation", metadata["x-operation-id"])

	restored := Append(context.Background(), metadata)
	require.Equal(t, "compute-task", GetTaskID(restored))
	require.Equal(t, "compute-operation", GetOperationID(restored))
}
