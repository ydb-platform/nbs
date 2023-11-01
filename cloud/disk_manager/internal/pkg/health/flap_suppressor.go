package health

import "sync/atomic"

////////////////////////////////////////////////////////////////////////////////

const maxCheckHistorySize = 21
const stableStateWindowSize = 7
const flapPercentThreshold = 0.31

////////////////////////////////////////////////////////////////////////////////

// Flap suppressor detects if the check is flapping and smoothes out the checks.
// The algoritm is the following:
// The flap is considered possible if there are any state changes within the last
// stableStateWindowSize checks.
// If there are no state changes within the last checks, the suppressor iterates
// the recorded history to see the percentage of the state changes.
// If there are more changes than the flapPercentThreshold - the check is flapping.
type flapSuppressor struct {
	checkHistory []bool
	checkStatus  *atomic.Bool
}

func newFlapSuppressor() flapSuppressor {
	status := new(atomic.Bool)
	status.Store(true)
	return flapSuppressor{
		checkHistory: make([]bool, 0, maxCheckHistorySize),
		checkStatus:  status,
	}
}

// Appends a new check state to the history,
// maintaining the maximum length of the history.
func (f *flapSuppressor) appendToHistory(checkState bool) {
	if len(f.checkHistory) == maxCheckHistorySize {
		f.checkHistory = f.checkHistory[1:]
	}

	f.checkHistory = append(f.checkHistory, checkState)
}

// Returns the smoothed status of the check.
func (f *flapSuppressor) status() bool {
	return f.checkStatus.Load()
}

func (f *flapSuppressor) detectFlapping() bool {
	var (
		lastState         bool
		stateChangesCount int
	)

	if len(f.checkHistory) < stableStateWindowSize {
		return true
	}

	stableWindowIdx := len(f.checkHistory) - stableStateWindowSize
	for idx, state := range f.checkHistory[stableWindowIdx:] {
		if idx == 0 {
			lastState = state
			continue
		}

		if lastState != state {
			return true
		}
	}

	for idx, state := range f.checkHistory {
		if idx == 0 {
			lastState = state
			continue
		}

		if lastState != state {
			stateChangesCount++
			lastState = state
		}
	}

	stateChangePercent := float64(stateChangesCount) /
		(float64(len(f.checkHistory)) - 1)
	return stateChangePercent >= flapPercentThreshold
}

////////////////////////////////////////////////////////////////////////////////

// Adds the check state to the suppressor history.
func (f *flapSuppressor) recordCheck(result bool) {
	f.appendToHistory(result)

	flapping := f.detectFlapping()

	// The check is flapping, hence we do not change the state of the check.
	if flapping {
		return
	}

	// The check is not flapping, hence we can change the status of the check if it changed.
	f.checkStatus.CompareAndSwap(!result, result)
}
