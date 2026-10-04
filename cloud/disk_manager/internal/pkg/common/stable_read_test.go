package common

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestStableReadIgnoresUnchangedContent(t *testing.T) {
	var stableRead stableRead[string]

	require.Equal(t, stableReadUnchanged, stableRead.observe("a", "a"))
}

func TestStableReadAppliesContentReadTwiceInARow(t *testing.T) {
	var stableRead stableRead[string]

	require.Equal(t, stableReadWait, stableRead.observe("a", "b"))
	require.Equal(t, stableReadApply, stableRead.observe("a", "b"))
}

func TestStableReadRestartsWhenContentChanges(t *testing.T) {
	var stableRead stableRead[string]

	stableRead.observe("a", "b")
	require.Equal(t, stableReadWait, stableRead.observe("a", "c"))
	require.Equal(t, stableReadApply, stableRead.observe("a", "c"))
}

func TestStableReadForgetsPendingContentWhenCurrentIsReadAgain(t *testing.T) {
	var stableRead stableRead[string]

	stableRead.observe("a", "b")
	require.Equal(t, stableReadUnchanged, stableRead.observe("a", "a"))

	// "b" is seen for the first time again.
	require.Equal(t, stableReadWait, stableRead.observe("a", "b"))
}

func TestStableReadResetsPendingContent(t *testing.T) {
	var stableRead stableRead[string]

	stableRead.observe("a", "b")
	stableRead.reset()
	require.Equal(t, stableReadWait, stableRead.observe("a", "b"))
}

func TestStableReadKeepsPendingContentWhenApplyIsRejected(t *testing.T) {
	var stableRead stableRead[string]

	stableRead.observe("a", "b")
	require.Equal(t, stableReadApply, stableRead.observe("a", "b"))

	// The caller failed to apply "b" and keeps "a": "b" stays pending and is
	// reported again on the next read.
	require.Equal(t, stableReadApply, stableRead.observe("a", "b"))
}
