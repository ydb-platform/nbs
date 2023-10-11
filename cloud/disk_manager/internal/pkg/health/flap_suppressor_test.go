package health

import (
	"testing"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

// Test that the suppressor detects the flickering flaps.
// E.g. true -> false -> true -> false -> ...
func TestFlapSuppressorDetectsFlickering(t *testing.T) {
	suppressor := newFlapSuppressor()

	for idx := 0; idx < maxCheckHistorySize; idx += 2 {
		suppressor.recordCheck(false)
		suppressor.recordCheck(true)
	}
	// The state of the suppressor has not changed.
	require.True(t, suppressor.status())
}

// Test that the suppressor eventually recognised the stable period
// and switches its state.
func TestFlapSuppressorDetectsStateChanges(t *testing.T) {
	suppressor := newFlapSuppressor()
	suppressor.recordCheck(true)
	suppressor.recordCheck(false)

	require.True(t, suppressor.status())

	for i := 0; i < maxCheckHistorySize-1; i++ {
		suppressor.recordCheck(false)
	}

	require.False(t, suppressor.status())

	suppressor.recordCheck(true)
	suppressor.recordCheck(true)

	require.False(t, suppressor.status())

	for i := 0; i < stableStateWindowSize-2; i++ {
		suppressor.recordCheck(true)
	}

	require.True(t, suppressor.status())
}

// Test that the suppressor must not flip when the history ends
// with the changed state.
// E.g. false -> ... -> false -> true.
func TestFlapSuppressorPreventsFlaps(t *testing.T) {
	suppressor := newFlapSuppressor()

	for idx := 0; idx < maxCheckHistorySize-1; idx += 1 {
		suppressor.recordCheck(false)
	}

	require.False(t, suppressor.status())

	suppressor.recordCheck(true)

	require.False(t, suppressor.status())
}

// Test that the suppressor recognized flickering periods
// within the recorded history followed by stable periods.
// E.g. true -> false -> true -> false -> false -> ... -> false.
func TestFlapSuppressorDetectsComplexFlaps(t *testing.T) {
	suppressor := newFlapSuppressor()

	// Unstable period true -> false -> true -> false
	for idx := 0; idx < maxCheckHistorySize; idx += 2 {
		suppressor.recordCheck(true)
		suppressor.recordCheck(false)
	}

	// During unstable period,
	// the suppressor does not change the state.
	require.True(t, suppressor.status())

	// In order for suppressor to switch,
	// the stable periods must be longer than 2/3 of the recorded history.
	// Hence, we have to overwrite the initial flickring true -> false -> true -> false
	// to make the suppressor switch.
	for idx := 0; idx < 2*stableStateWindowSize; idx++ {
		suppressor.recordCheck(false)
	}

	require.False(t, suppressor.status())
}
