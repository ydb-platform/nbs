package headers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

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
